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
import com.google.api.client.googleapis.util.Utils;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Utilities for parsing and formatting strings.
 */
public class StringUtils {

  /**
   * Parse command-line options of the form --key=value into a map of <key,value>. For --key without
   * a value, set the value to true.
   *
   * @param args
   * @return
   */
  public static Map<String, String> parseArgs(String[] args) {
    Map<String, String> m = new HashMap<String, String>();
    if (args != null) {
      for (String s : args) {
        if (s.indexOf("=") < 0) {
          m.put(s.replace("--", ""), Boolean.TRUE.toString());
        } else {
          String key = s.substring(0, s.indexOf("=")).replace("--", "");
          String val = s.substring(s.indexOf("=") + 1);
          m.put(key, val);
        }
      }
    }
    return m;
  }

  /**
   * Substitute all global variables of the form $(KEY) in a string.
   *
   * @param globals
   * @param value
   * @return
   */
  public static String replaceAll(Map<String, String> globals, String value) {
    String retval = value;

    if (value != null && globals != null) {
      for (String key : globals.keySet()) {
        String var = "${" + key + "}";
        if (value.contains(var)
            && globals.get(key) != null) {
          retval = retval.replace(var, globals.get(key));
        }
      }
    }
    return retval;
  }

  /** Serialize to json. */
  public static String toJson(Object o) {
    FileUtils.LOG.debug("Serializing to json: " + (o == null ? null : o.getClass()));
    // For non-auto-generated Google Java classes, Gson is required;
    // otherwise the serialized string is empty.
    return new GsonBuilder().setPrettyPrinting().create().toJson(o);
  }

  /** Deserialize from json. */
  public static <T> T fromJson(String s, Class<T> c) throws IOException {
    FileUtils.LOG.debug("Deserializing from json to " + c);
    T retval;

    // For some reason, this only works for auto-generated Google API
    // classes
    if (c.toString().startsWith("com.google.api.services.")) {
      FileUtils.LOG.debug("Using Google APIs JsonParser");
      retval = Utils.getDefaultJsonFactory().createJsonParser(s).parse(c);
    } else {
      FileUtils.LOG.debug("Using Gson");
      retval = new GsonBuilder().setLenient().create().fromJson(s, c);
    }
    return retval;
  }

  /**
   * Parse parameters of the form key=val,key2=val2.
   *
   * @param params
   * @param fromFile if true, read the file path contents and set as the parameter value
   * @return
   * @throws IOException
   */
  public static Map<String, String> parseParameters(String params, boolean fromFile)
      throws IOException {
    Map<String, String> map = new HashMap<String, String>();

    String[] pairs = params.split(",");

    for (String pair : pairs) {
      String[] keyValue = pair.split("=");
      if (keyValue.length != 2) {
        throw new IllegalArgumentException("Invalid parameter: " + pair);
      }
      String value = keyValue[1];
      if (fromFile) {
        value = FileUtils.readAll(value);
      }
      map.put(keyValue[0], value);
    }
    return map;
  }

  /**
   * Evaluate a javascript expression, like "${= 2*3}".
   *
   * @param js
   * @return the results as a string.
   */
  public static String evalJavaScript(String expression) throws ScriptException {
    FileUtils.LOG.debug("javascript: " + expression);

    // Remove new lines from arrays, etc
    String s = expression.trim().replace("\n", " ");
    StringBuilder sb = new StringBuilder();

    int start = s.indexOf("${=") + 3;

    // Keep text before the js
    if (start > 3) {
      sb.append(s.substring(0, start - 3));
    }
    int end = s.indexOf("}", start);
    FileUtils.LOG.debug("start=" + start + ", end=" + end);

    String js = s.substring(start, end);
    FileUtils.LOG.info("Evaluate js: " + js);

    sb.append(
        String.valueOf(new ScriptEngineManager().getEngineByName("JavaScript").eval(js)).trim());

    // Keep text after the js
    if (end < s.length() - 1) {
      sb.append(s.substring(end + 1));
    }

    String retval = sb.toString();

    // If there's more js, evaluate it too
    if (StringUtils.isJavaScript(retval)) {
      retval = evalJavaScript(retval);
    }

    return retval;
  }

  /**
   * The value looks like "${= javascript_expression }". It must start with a dollar sign and end
   * with a curly brace -- ie, JavaScript cannot be embedded within a longer string.
   *
   * @param js
   * @return
   */
  public static boolean isJavaScript(String js) {
    return js != null
        && js.contains("${=")
        && js.contains("}")
        && js.indexOf("${=") < js.indexOf("}");
  }

  public static String toYaml(Object o) throws IOException {
    // Round trip to json to suppress empty collections and null values
    String json = toJson(o);
    Object generic = fromJson(json, Object.class);
    return new Yaml().dump(generic);
  }
}
