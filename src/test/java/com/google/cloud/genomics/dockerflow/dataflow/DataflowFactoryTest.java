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
package com.google.cloud.genomics.dockerflow.dataflow;

import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.TestUtils;
import com.google.cloud.genomics.dockerflow.runner.TaskRunner.TaskRequest;
import com.google.cloud.genomics.dockerflow.task.TaskDefn;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for parsing workflows and 
 */
public class DataflowFactoryTest implements DockerflowConstants {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowFactoryTest.class);
  private static TestUtils utils = new TestUtils();

  @Test
  public void testLoadTask() throws IOException {
    WorkflowFactory.loadDefn(utils.baseDir + "/task-one.yaml");
  }

  @Test
  public void testParseTask() throws IOException {
    TaskDefn d = FileUtils.parseFile(utils.baseDir + "/task-one.yaml", TaskDefn.class);
    LOG.info("Round trip as: " + StringUtils.toJson(d));
  }

  @Test
  public void testParseWorkflowWithParams() throws IOException {
    Workflow w = FileUtils.parseFile(utils.baseDir + "/linear-graph.yaml", Workflow.class);
    LOG.info("Round trip as: " + StringUtils.toJson(w));
  }

  @Test
  public void testParseWorkflow() throws IOException {
    Workflow w = FileUtils.parseFile(utils.baseDir + "/workflow.yaml", Workflow.class);
    LOG.info("Round trip as: " + StringUtils.toJson(w));
  }

  @Test
  public void testLoadWorkflow() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/linear-graph.yaml");
    LOG.info("Loaded workflow " + w.getDefn().getName());
  }

  @Test
  public void testCreateLinearDataflow() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/linear-graph.yaml");
    Pipeline p = DataflowFactory.dataflow(w, null, DataflowFactory.pipelineOptions(
        new String[] {
            "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
            "--" + STAGING + "=" + utils.baseDir + "/dataflow",
            "--" + LOGGING + "=" + utils.baseDir + "/dlinear",
            "--" + RUNNER + "=" + utils.runner
            }
    ));
    LOG.info("Created dataflow pipeline: " + p);
  }

  @Test
  public void testCreateBranchingDataflow() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/branching-graph.yaml");
    Pipeline p = DataflowFactory.dataflow(w, null, DataflowFactory.pipelineOptions(
        new String[] {
            "--" + PROJECT + "=" + TestUtils.TEST_PROJECT,
            "--" + STAGING + "=" + utils.baseDir + "/dataflow",
            "--" + LOGGING + "=" + utils.baseDir + "/dlinear",
            "--" + RUNNER + "=" + utils.runner
            }
    ));
    LOG.info("Created dataflow pipeline: " + p);
  }

  @Test
  public void testRequestToJson() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/linear-graph.yaml");
    TaskRequest r = new TaskRequest();
    r.setEphemeralPipeline(w.getDefn());
    r.setPipelineArgs(w.getArgs());

    String s = StringUtils.toJson(r);
    LOG.info("Serialized: " + s);

    r = StringUtils.fromJson(s, TaskRequest.class);
    LOG.info("Deserialized");
  }

  @Test
  public void testWorkflowToJson() throws Exception {
    Workflow w = WorkflowFactory.load(utils.baseDir + "/linear-graph.yaml");

    String s = StringUtils.toJson(w);
    LOG.info("Serialized: " + s);

    w = StringUtils.fromJson(s, Workflow.class);
    LOG.info("Deserialized");
  }

  @Test
  public void testZones() throws IOException {
    List<String> s = WorkflowFactory.expandZones(new String[] {"us-*"});
    LOG.info(s.toString());
    assertTrue(s.size() > 1);
  }

  @Test
  public void testJavascript() throws Exception {
    String s = StringUtils.evalJavaScript("${= 2 + 3 * 4}");
    LOG.info(s);
    assertTrue(s.equals("14"));
  }

  @Test
  public void testResolveLoggingPaths() {
    String s;

    s = FileUtils.logPath("gs://foo/bar", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/123.log"));

    s = FileUtils.stdoutPath("gs://foo/bar", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/123-stdout.log"));

    s = FileUtils.stderrPath("gs://foo/bar", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/123-stderr.log"));

    s = FileUtils.logPath("gs://foo/bar/log.txt", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/log.txt"));

    s = FileUtils.stdoutPath("gs://foo/bar/log.txt", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/log-stdout.txt"));

    s = FileUtils.stderrPath("gs://foo/bar/log.txt", "operations/123");
    LOG.info(s);
    assertTrue(s.equals("gs://foo/bar/log-stderr.txt"));
  }
}
