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
package com.google.cloud.genomics.dockerflow.examples;

import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.TestUtils;
import com.google.cloud.genomics.dockerflow.args.ArgsBuilder;
import com.google.cloud.genomics.dockerflow.args.TaskArgs;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.runner.TaskRunner;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests.
 * 
 * <p>You need to set environment variables: TEST_PROJECT and TEST_GCS_PATH. Then get application
 * default credentials:
 *
 * <pre>gcloud beta auth application-default login</pre>
 */
public class ExampleGraphsTest implements DockerflowConstants {
  private static Logger LOG = LoggerFactory.getLogger(ExampleGraphsITCase.class);

  protected TestUtils utils = new TestUtils();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @Test
  public void testTask() throws Exception {
    Task t =
        TaskBuilder.named("TaskOne")
            .project(TestUtils.TEST_PROJECT)
            .logging(utils.baseDir + "/dtask/test.log")
            .inputFile("inputFile", utils.baseDir + "/input-one.txt", "in.txt")
            .input("message", "hello")
            .outputFile("outputFile", utils.baseDir + "/dtask/output.txt", "out.txt")
            .docker("ubuntu")
            .script("cp ${inputFile} ${outputFile} ; echo ${message} >> ${outputFile}")
            .zones(new String[] {"us-*"})
            .build();
    ((WorkflowArgs) t.getArgs()).setTesting(DockerflowConstants.DIRECT_RUNNER.equals(utils.runner));
    TaskRunner.runTask(t);
  }

  @Test
  public void testLinearGraph() throws Exception {
    utils.baseDir = TestUtils.TEST_GCS_PATH;
    LinearGraph.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + LOGGING + "=" + utils.baseDir + "/dlinear",
          "--" + RUNNER + "=" + utils.runner,
          "--inputFile=" + utils.baseDir + "/input-one.txt",
          "--outputFile=" + utils.baseDir + "/dlinear/output.txt",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner)
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/dlinear/output.txt");
      LOG.info("\"" + output + "\", length=" + output.length());

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_ONE_TWO, output);
    }
  }

  @Test
  public void testMultiLinearGraph() throws Exception {
    MultiLinearGraph.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + LOGGING + "=" + utils.baseDir,
          "--" + RUNNER + "=" + utils.runner,
          "--" + WORKSPACE + "=" + utils.baseDir,
          "--" + ARGS_FILE + "=" + utils.baseDir + "/workflowArgs.csv",
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner)
        });
    if (utils.checkOutput) {
      String output1 = TestUtils.readAll(utils.baseDir + "/1/TaskOne/output-one.txt");
      LOG.info("\"" + output1 + "\", length=" + output1.length());

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_ONE, output1);

      String output2 = TestUtils.readAll(utils.baseDir + "/2/TaskTwo/output-two.txt");
      LOG.info("\"" + output2 + "\", length=" + output2.length());

      assertEquals("Output doesn't match expected", TestUtils.OUTPUT_TWO_ONE, output2);
    }
  }

  @Test
  public void testComplexGraph() throws Exception {
    ComplexGraph.main(
        new String[] {
          "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
          "--" + STAGING + "=" + utils.baseDir + "/dataflow",
          "--" + LOGGING + "=" + utils.baseDir + "/dcomplex",
          "--" + RUNNER + "=" + utils.runner,
          "--" + TEST + "=" + DockerflowConstants.DIRECT_RUNNER.equals(utils.runner)
        });
    if (utils.checkOutput) {
      String output = TestUtils.readAll(utils.baseDir + "/dcomplex/test.log");
      LOG.info("\"" + output);
    }
  }

  @Test
  public void testCwlFeatures() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/cwl-graph.yaml");
    LOG.info("Loaded workflow " + w.getDefn().getName());

    Task t = w.getSteps().get(0);
    t.applyArgs(new TaskArgs(t.getArgs()));
    LOG.info(StringUtils.toJson(t));

    assertEquals(
        t.getDefn().getDocker().getCmd(),
        "echo -a one -a two -b -k five -k six -k seven --file\u003dgs://b/d/test.txt");
  }

  @SuppressWarnings("serial")
  @Test
  public void testJs() throws Exception {
    String path =
        "gs://genomics-public-data/test-data/dna/wgs/hiseqx/NA12878/H06HDADXX130110.1.ATCACGAT.20k_reads.bam";
    String js = "${= '${path}'.replace(/.*\\//, '').replace(/.bam/, ''); }";

    String sub =
        StringUtils.replaceAll(
            new HashMap<String, String>() {
              {
                put("path", path);
              }
            },
            js);
    LOG.info(sub);

    String eval = StringUtils.evalJavaScript(sub);
    LOG.info(eval);
  }

  @Test
  public void testScatterByArray() throws Exception {
    Task t =
        TaskBuilder.named("ArrayTest")
            .inputArray("sequence_group_interval", " -L ")
            .scatterBy("sequence_group_interval")
            .docker("ubuntu")
            .script("echo hello")
            .build();
    t.getArgs().setFromFile("sequence_group_interval", true);
    LOG.info(StringUtils.toJson(t));

    WorkflowArgs wa =
        ArgsBuilder.of()
            .inputFromFile("ArrayTest.sequence_group_interval", utils.baseDir + "/seq-group.tsv")
            .workspace(utils.baseDir)
            .build();
    LOG.info(StringUtils.toJson(wa));

    t.applyArgs(wa);

    Object r = TaskRunner.getRequest(t);

    LOG.info(StringUtils.toJson(r));
  }
}
