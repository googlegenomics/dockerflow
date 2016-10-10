/*
 * Copyright 2016 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dockerflow.util;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods used with the Dataflow Docker example and the Pipelines API runner. */
public class FileUtils {
  static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  /**
   * Read a GCS file into a string.
   *
   * @param gcsPath
   * @return file contents
   * @throws IOException
   */
  public static String readFromGcs(String gcsPath) throws IOException {
    if (gcsPath == null || !gcsPath.startsWith("gs://")) {
      throw new IllegalArgumentException("GCS path must be non-null and start with gs://.");
    }
    return HttpUtils.doGet(gcsUrl(gcsPath));
  }

  /** Get the HTTP URL for a GCS path. */
  static String gcsUrl(String gcsPath) {
    return "https://storage.googleapis.com/" + gcsPath.substring("gs://".length());
  }

  /** Read complete stream into a string. */
  public static String readAll(InputStream is) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(is));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = in.readLine()) != null) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(line);
    }
    in.close();
    return sb.toString();
  }

  /**
   * Read the contents of a local or GCS path.
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static String readAll(String path) throws IOException {
    String text;
    if (path.startsWith("gs://")) {
      text = readFromGcs(path);
    } else {
      text = readAll(new FileInputStream(path));
    }
    return text;
  }

  /**
   * Load a file from GCS or local and parse from json or yaml.
   *
   * @param <T>
   */
  @SuppressWarnings("unchecked")
  public static <T> T parseFile(String path, Class<T> c) throws IOException {
    LOG.info("Parse file from path: " + path + " for class " + c);

    String text = readAll(path);

    // Ridiculous hack: direct parsing into a real Java object fails with
    // SnakeYaml, Gson and Jackson due to mysterious type incompatibility :(
    Map<String, Object> map;
    if (path.endsWith("yaml") || path.endsWith("yml")) {
      map = (Map<String, Object>) new Yaml().load(text);

    } else {
      map =
          (Map<String, Object>)
              new GsonBuilder()
                  .setLenient()
                  .create()
                  .fromJson(text, new TypeToken<Map<String, Object>>() {}.getType());
    }

    String s = StringUtils.toJson(map);
    return StringUtils.fromJson(s, c);
  }

  /**
   * Resolve a possibly local path based on a parent directory path. The parent path *must* end with
   * a "/" if it's a directory. Otherwise it will be removed.
   *
   * @param path a possibly local path
   * @param parent a file or a directory ending in a "/"
   * @return resolved path
   * @throws URISyntaxException
   */
  public static String resolve(String path, String parent) 
      throws IOException, URISyntaxException {
    LOG.debug("Resolve " + path + " vs parent " + parent);
    String retval;

    if (path.startsWith("/") || path.startsWith("gs:/")) {
      retval = path;
    } else if (parent.endsWith("/")) {
      retval = parent + path;
    } else {
      retval = parent.substring(0, parent.lastIndexOf("/") + 1) + path;
    }
    if (retval.startsWith("gs:/")) {
      retval = new URI(retval).normalize().toString();
    } else {
      retval = new File(retval).getCanonicalPath();
    }
    return retval;
  }

  /**
   * A local path or file name on local disk.
   *
   * @param gcsPath GCS path or, if testing locally, a path on the local machine
   */
  public static String localPath(String gcsPath) {
    if (gcsPath == null) {
      return null;
    }
    String localPath = gcsPath;

    // If multiple files, store in the root of the mounted drive
    if (gcsPath.indexOf("*") > 0 || gcsPath.split("\\s+").length > 1) {
      localPath = "";
    // Otherwise store by the input file name, prefixed with a hashcode
    // for the parent path, so files in the same directory preserve name relations.
    // Eg, file.tar and file.tar.gz.
    } else {
      String dir = gcsPath.contains("/") ? gcsPath.substring(0, gcsPath.lastIndexOf("/")) : gcsPath;
      localPath =
          String.valueOf(dir.hashCode()).replace("-", "")
              + "-"
              + gcsPath.substring(gcsPath.lastIndexOf("/") + 1);
    }

    return localPath;
  }

  /**
   * The resolved path where the Pipelines API will copy the task log.
   *
   * @param path the GCS path passed to the Pipelines API
   * @param operationName the operation name, or null if unknown
   */
  public static String logPath(String path, String operationName) {
    return logPath(path, operationName, "");
  }

  /**
   * The resolved path where the Pipelines API will copy the contents of stdout.
   *
   * @param path the GCS path passed to the Pipelines API
   * @param operationName the operation name, or null if unknown
   */
  public static String stdoutPath(String path, String operationName) {
    return logPath(path, operationName, "-stdout");
  }

  /**
   * The path where the Pipelines API will copy the contents of stdout.
   *
   * @param path the GCS path passed to the Pipelines API
   * @param operationName the operation name, or null if unknown
   */
  public static String stderrPath(String path, String operationName) {
    return logPath(path, operationName, "-stderr");
  }

  /**
   * The resolved path where the Pipelines API will copy the contents of stdout.
   *
   * @param path the GCS path passed to the Pipelines API
   * @param operationName the operation name, or null if unknown
   * @param logName: one of empty string, "-stderr" or "-stdout"
   */
  private static String logPath(String path, String operationName, String logName) {
    if (path == null) {
      throw new IllegalArgumentException("Path cannot be null");
    }

    String retval = path;
    if (path.matches("^(.*\\.\\w*)$")) {
      retval =
          path.substring(0, path.lastIndexOf("."))
              + logName
              + path.substring(path.lastIndexOf("."));
    } else {
      if (operationName == null) {
        throw new IllegalArgumentException(
            "operationName cannot be null if logging path is a directory");
      }
      retval =
          path + "/" + operationName.substring(operationName.indexOf("/") + 1) + logName + ".log";
    }
    return retval;
  }

  /**
   * Check if a URI path exists.
   *
   * @param path GCS path, typically
   * @return true if the path exists
   */
  public static boolean gcsPathExists(String gcsPath) {
    boolean exists;
    if (gcsPath == null) {
      exists = false;
    } else {
      try {
        HttpUtils.doHead(gcsUrl(gcsPath));
        exists = true;
      } catch (IOException e) {
        LOG.info(e.getMessage());
        exists = false;
      }
    }
    return exists;
  }
}
