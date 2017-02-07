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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests. Runs  the same tests as the superclass, but remotely with the
 * Dataflow service and Pipelines API.
 * 
 * <p>To run the tests, gsutil cp all of the src/test/resources files to TEST_GCS_PATH.
 * 
 * <p>After running integration tests, clean up by running:
 * <pre>gsutil -m rm -r TEST_BASE_DIR/*</pre>
 */
public class DockerflowITCase extends DockerflowTest {
  private static final Logger LOG = LoggerFactory.getLogger(DockerflowITCase.class);

  public DockerflowITCase() {
    utils.baseDir = TestUtils.TEST_GCS_PATH;
    utils.runner = DockerflowConstants.BLOCKING_RUNNER;
    utils.checkOutput = true;
    LOG.info("Running with GCS paths, blocking runner, and output file checks enabled");
  }

  @Test
  @Override
  public void testSingleTaskNoWait() throws Exception {
    utils.runner = DockerflowConstants.DEFAULT_RUNNER;
    super.testSingleTaskNoWait();
    utils.runner = DockerflowConstants.BLOCKING_RUNNER;
  }

  @Test
  @Override
  public void testSingleTask() throws Exception {
    super.testSingleTask();
  }

  @Test
  @Override
  public void testParameterSubstitution() throws Exception {
    super.testParameterSubstitution();
  }

  @Test
  @Override
  public void testLinearGraph() throws Exception {
    super.testLinearGraph();
  }

  @Test
  @Override
  public void testScatter() throws Exception {
    super.testScatter();
  }

  @Test
  @Override
  public void testGather() throws Exception {
    super.testGather();
  }

  @Test
  @Override
  public void testReorderedGraph() throws Exception {
    super.testReorderedGraph();
  }

  @Test
  @Override
  public void testBranchingGraph() throws Exception {
    super.testBranchingGraph(); 
  }

  @Test
  @Override
  public void testComplexGraph() throws Exception {
    super.testComplexGraph();
  }

  @Test
  @Override
  public void testFolderCopy() throws Exception {
    super.testFolderCopy();
  }
}
