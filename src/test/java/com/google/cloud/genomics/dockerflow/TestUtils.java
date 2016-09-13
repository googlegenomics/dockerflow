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
package com.google.cloud.genomics.dockerflow;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.HttpUtils;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for testing. */
public class TestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  // Environment variables
  public static final String TEST_PROJECT = System.getenv("TEST_PROJECT");
  public static final String TEST_GCS_PATH = System.getenv("TEST_GCS_PATH");
  public static final String RESOURCE_DIR = "src/test/resources";

  public String baseDir = RESOURCE_DIR; // Allow local or GCS paths
  public String runner = DockerflowConstants.DIRECT_RUNNER;
  public boolean checkOutput = false; // because we can't yet on local files

  // Expected results
  public static final String OUTPUT_ONE = "cat\nhello";
  public static final String OUTPUT_TWO = "dog\nhello";
  public static final String OUTPUT_ONE_TWO = "cat\nhello\ngoodbye";
  public static final String OUTPUT_TWO_ONE = "dog\ngoodbye\nhello";
  public static final String OUTPUT_ONE_TWO_THREE = "cat\nhello\ngoodbye\nhello";

  public TestUtils() {
    assertNotNull("You must set the TEST_PROJECT environment variable.", TestUtils.TEST_PROJECT);
    assertNotNull("You must set the TEST_GCS_PATH environment variable.", TestUtils.TEST_GCS_PATH);
    assertTrue("TEST_GCS_PATH must begin with gs:// ", TestUtils.TEST_GCS_PATH.startsWith("gs://"));
    assertTrue(
        "TEST_GCS_PATH must not end with a trailing slash /",
        !TestUtils.TEST_GCS_PATH.endsWith("/"));

    LOG.info("TEST_PROJECT=" + TestUtils.TEST_PROJECT);
  }

  /**
   * Read file contents with retries, since GCS is eventually consistent, and output files may not
   * be visible right away.
   */
  public static String readAll(String path) {
    String output = null;
    final int maxTries = 1;
    int attempt = 0;
    do {
      DockerflowTest.LOG.info("Reading output from " + path);
      try {
        output = FileUtils.readAll(path);
      } catch (IOException e) {
        DockerflowTest.LOG.info("Failed attempt " + attempt + " with error: " + e.getMessage());
        ++attempt;
        try {
          DockerflowTest.LOG.info("Sleeping for 20 sec");
          TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException i) {
          // ignore
        }
      }
    } while (output == null && attempt < maxTries);
    return output;
  }

  public static void delete(String gcsPath) {
    if (!gcsPath.startsWith("gs://")) {
      if (gcsPath.startsWith("/")) {
        gcsPath = TestUtils.TEST_GCS_PATH + gcsPath;
      } else {
        gcsPath = TestUtils.TEST_GCS_PATH + "/" + gcsPath;
      }
    }
    try {
      HttpUtils.doDelete(gcsPath);
    } catch (IOException e) {
      DockerflowTest.LOG.info("Failed to delete: " + gcsPath);
    }
  }
}
