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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests.
 *
 * <p>You need to set environment variables: TEST_PROJECT and TEST_GCS_PATH. Then get
 * application default credentials:
 *
 * <pre>gcloud beta auth application-default login</pre>
 */
public class DockerflowTest implements DockerflowConstants {
  static final Logger LOG = LoggerFactory.getLogger(DockerflowTest.class);

  protected TestUtils utils = new TestUtils();

  @Test
  public void testHelp() throws Exception {
    Dockerflow.main(new String[] {"--help"});
  }

  @Test
  public void testSingleTaskNoWait() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + TASK_FILE + "=" + utils.baseDir + "/task-one.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/async/test.log",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + INPUTS + "=TaskOne.inputFile=" + utils.baseDir + "/input-one.txt",
          "--" + OUTPUTS + "=TaskOne.outputFile=" + utils.baseDir + "/async/output-one.txt",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      try {
        FileUtils.readAll(utils.baseDir + "/async/output-one.txt");
        fail();
      } catch (IOException e) {
        assertTrue(e.getMessage().startsWith("HTTP error: 404"));
      }
    }
  }

  @Test
  public void testSingleTask() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + TASK_FILE + "=" + utils.baseDir + "/task-one.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/task/test.log",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + INPUTS + "=TaskOne.inputFile=" + utils.baseDir + "/input-one.txt",
          "--" + OUTPUTS + "=TaskOne.outputFile=" + utils.baseDir + "/task/output-one.txt",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/task/output-one.txt");
      LOG.info("\"" + output + "\", length=" + output.length());

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_ONE, output);
    }
  }

  @Test
  public void testParameterSubstitution() throws Exception {
    String[] args =
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/param-sub.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/task",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + INPUTS + "=stepOne.inputFile=" + utils.baseDir + "/input-two.txt,"
              + "BASE_DIR=" + utils.baseDir + "/task",
          "--" + OUTPUTS + "=stepOne.outputFile=" + utils.baseDir + "/task/output-two.txt",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        };
    Workflow w = WorkflowFactory.create(args);
    WorkflowArgs wa = WorkflowFactory.createArgs(args);
    w.setArgs(wa);
    w.applyArgs(wa);

    String s = StringUtils.toJson(w);
    LOG.info(s);

    assertTrue("Search and replace of globals failed", s.indexOf("${BASE_DIR}") < 0);
  }

  @Test
  public void testLinearGraph() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/linear-graph.yaml",
          "--" + WORKSPACE + "=" + utils.baseDir + "/linear",
          "--" + INPUTS + "=BASE_DIR=" + utils.baseDir,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/linear/stepTwo/output-two.txt");
      LOG.info("\"" + output + "\"");

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_ONE_TWO, output);
    }
  }

  @Test
  public void testScatter() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/parallel-graph.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/parallel",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + WORKSPACE + "=" + utils.baseDir + "/parallel",
          "--" + INPUTS + "=BASE_DIR=" + utils.baseDir,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/parallel/stepOne/1/output-one.txt");
      LOG.info("\"" + output + "\"");

      if (!TestUtils.OUTPUT_ONE.equals(output) && !TestUtils.OUTPUT_TWO.equals(output)) {
        fail("Output doesn't match expected");
      }
    }
  }

  @Test
  public void testGather() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/gather-graph.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/gather",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + WORKSPACE + "=" + utils.baseDir + "/gather",
          "--" + INPUTS + "=BASE_DIR=" + utils.baseDir,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/gather/stepTwo/output-two.txt");
      LOG.info("\"" + output + "\"");

      if (!TestUtils.OUTPUT_ONE_TWO.equals(output)) {
        fail("Output doesn't match expected");
      }
    }
  }

  @Test
  public void testReorderedGraph() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/reordered-graph.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/reordered",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + WORKSPACE + "=" + utils.baseDir + "/reordered",
          "--" + INPUTS + "=BASE_DIR=" + utils.baseDir,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/reordered/output-one.txt");
      LOG.info("\"" + output + "\"");

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_TWO_ONE, output);
    }
  }

  @Test
  public void testBranchingGraph() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/branching-graph.yaml",
          "--" + LOGGING + "=" + utils.baseDir + "/branching",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + INPUTS + "=BASE_DIR=" + utils.baseDir,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/branching/output-three.txt");
      LOG.info("\"" + output + "\"");

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_ONE_TWO, output);
    }
  }

  @Test
  public void testComplexGraph() throws Exception {
    Dockerflow.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/complex-graph.yaml",
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + LOGGING + "=" + utils.baseDir + "/complex",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
          "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/complex/stepSix/task.log");
      LOG.info("\"" + output);
    }
  }
  
  @Test
  public void testFolderCopy() throws Exception {
    Dockerflow.main(
        new String[] {
            "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
            "--" + WORKFLOW_FILE + "=" + utils.baseDir + "/folder-copy.yaml",
            "--" + WORKSPACE + "=" + utils.baseDir + "/folder-copy",
            "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner),
            "--" + INPUTS + "=stepOne.inputFolder=../../test-folder",
            "--" + OUTPUTS + "=stepOne.outputFolder=test-output",
            "--" + STAGING + "=" + utils.baseDir + "/dataflow",
            "--" + RUNNER + "=" + utils.runner
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/folder-copy/stepOne/test-output/file1.txt");
      LOG.info("\"" + output + "\"");
      
      assertEquals("Folder copy failed", TestUtils.OUTPUT_ONE, output);
    }
  }

  @Test
  public void testFileParsing() throws Exception {
    Map<String, String> m = StringUtils.parseParameters("key_1=${val_1},key_2=${val_2}", false);
    
    assertEquals("Wrong number of keys", 2, m.size());
  }

  @Test
  public void testArrayParsing() throws Exception {
    Map<String, String> m = StringUtils.parseParameters("foo[\" sep=val \"]=bar", false);
    
    assertEquals("Wrong number of keys", 1, m.size());
    assertEquals("Array key parsed incorrectly", "foo[\" sep=val \"]", m.keySet().iterator().next());
  }
}
