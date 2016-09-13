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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.gson.GsonBuilder;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
  static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

  /**
   * Do an HTTPS GET by appending the application default token to the URL.
   *
   * @param url
   * @return response body as string
   * @throws IOException
   */
  public static String doGet(String url) throws IOException {
    LOG.debug("Url for GET: " + url);
    String authUrl =
        url + "?access_token=" + GoogleCredential.getApplicationDefault().getAccessToken();

    HttpURLConnection con = (HttpURLConnection) new URL(authUrl).openConnection();
    con.setDoInput(true);
    con.setRequestMethod("GET");

    int code = con.getResponseCode();
    LOG.debug("Response code: " + code);

    if (code != HttpURLConnection.HTTP_OK) {
      String msg = FileUtils.readAll(con.getErrorStream());
      throw new IOException("HTTP error: " + code + " for url " + url + "" + msg);
    }
    return FileUtils.readAll(con.getInputStream());
  }

  /**
   * Do an HTTPS POST with a JSON object as the request body. Append the application default token
   * to the URL for authentication.
   *
   * @param url
   * @param param object to encode as JSON in the request body
   * @return response body as string
   * @throws IOException
   */
  public static String doPost(String url, Object param) throws IOException {
    LOG.debug("Url for POST: " + url);
    String authUrl =
        url + "?access_token=" + GoogleCredential.getApplicationDefault().getAccessToken();

    HttpURLConnection con = (HttpURLConnection) new URL(authUrl).openConnection();
    con.setDoOutput(true);
    con.setDoInput(true);
    con.setRequestMethod("POST");
    con.setRequestProperty("Content-Type", "application/json");

    DataOutputStream out = new DataOutputStream(con.getOutputStream());
    String params = new GsonBuilder().create().toJson(param);
    out.writeBytes(params);
    out.close();
    LOG.debug(params);

    int code = con.getResponseCode();
    LOG.debug("Response code: " + code);

    if (code != HttpURLConnection.HTTP_OK) {
      String msg = FileUtils.readAll(con.getErrorStream());
      throw new IOException("HTTP error: " + code + " for url " + url + "" + msg);
    }
    return FileUtils.readAll(con.getInputStream());
  }

  /**
   * Do an HTTPS DELETE by appending the application default token to the URL.
   *
   * @param url
   * @return response body as string
   * @throws IOException
   */
  public static String doDelete(String gcsPath) throws IOException {
    if (gcsPath == null || !gcsPath.startsWith("gs://")) {
      throw new IOException("GCS path must be non-null and start with gs://. Value: " + gcsPath);
    }
    String url = "https://storage.googleapis.com/" + gcsPath.substring("gs://".length());
    LOG.debug("Url for DELETE: " + url);

    String authUrl =
        url + "?access_token=" + GoogleCredential.getApplicationDefault().getAccessToken();

    HttpURLConnection con = (HttpURLConnection) new URL(authUrl).openConnection();
    con.setDoInput(true);
    con.setRequestMethod("DELETE");

    int code = con.getResponseCode();
    LOG.debug("Response code: " + code);

    if (code != HttpURLConnection.HTTP_OK && code != HttpURLConnection.HTTP_NO_CONTENT) {
      String msg = FileUtils.readAll(con.getErrorStream());
      throw new IOException("HTTP error: " + code + " for url " + url + "" + msg);
    }
    return FileUtils.readAll(con.getInputStream());
  }

  /**
   * Do an HTTPS HEAD by appending the application default token to the URL.
   *
   * @param url
   * @return response body as string
   * @throws IOException
   */
  public static String doHead(String url) throws IOException {
    LOG.debug("Url for HEAD: " + url);
    String authUrl =
        url + "?access_token=" + GoogleCredential.getApplicationDefault().getAccessToken();

    HttpURLConnection con = (HttpURLConnection) new URL(authUrl).openConnection();
    con.setDoInput(true);
    con.setRequestMethod("HEAD");

    int code = con.getResponseCode();
    LOG.debug("Response code: " + code);

    if (code != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP error: " + code + " for url ");
    }
    return FileUtils.readAll(con.getInputStream());
  }
}
