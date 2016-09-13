/*
 * Copyright 2016 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dockerflow.examples;

import static org.junit.Assert.assertTrue;

import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.TestUtils;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import java.io.IOException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests. Runs the same tests as the superclass, but remotely with Dataflow's blocking
 * runner.
 * <p> To run, you'll need to gsutil cp all of the src/test/resources files to TEST_GCS_PATH.
 * 
 * <p>After running integration tests, clean up by running:
 * <pre>gsutil -m rm -r TEST_BASE_DIR/*</pre>
 */
public class ExampleGraphsITCase extends ExampleGraphsTest {
  private static Logger LOG = LoggerFactory.getLogger(ExampleGraphsITCase.class);

  public ExampleGraphsITCase() throws IOException {
    utils.baseDir = TestUtils.TEST_GCS_PATH;
    utils.runner = DockerflowConstants.BLOCKING_RUNNER;
    utils.checkOutput = true;
    LOG.info("Running with GCS paths, blocking runner, and output file checks enabled");
  }

  @Test
  @Override
  public void testTask() throws Exception {
    super.testTask();
  }

  @Test
  @Override
  public void testLinearGraph() throws Exception {
    super.testLinearGraph();
  }

  @Test
  @Override
  public void testMultiLinearGraph() throws Exception {
    super.testMultiLinearGraph();
  }

  @Test
  @Override
  public void testComplexGraph() throws Exception {
    super.testComplexGraph();
  }

  @Test
  public void testFileExists() throws Exception {
    assertTrue(FileUtils.gcsPathExists(utils.baseDir + "/task-one.yaml"));
  }
}
