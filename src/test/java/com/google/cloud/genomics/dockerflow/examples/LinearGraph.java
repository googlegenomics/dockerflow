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

import com.google.cloud.genomics.dockerflow.dataflow.DataflowBuilder;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a simple two-step workflow with Docker steps using Dataflow for orchestration.
 *
 * <p>Required command-line arguments:
 *
 * <pre>
 * --project=YOUR_PROJECT_ID
 * --workspace=gs://YOUR_BUCKET/DIR
 * --inputFile=FILE
 * --outputFile=FILE
 * --runner=DATAFLOW_RUNNER_NAME
 * </pre>
 */
public class LinearGraph {
  private static final Logger LOG = LoggerFactory.getLogger(LinearGraph.class);

  public static void main(String[] args) throws IOException {

    LOG.info("Parsing command-line arguments");
    Map<String, String> m = StringUtils.parseArgs(args);
    String inputFile = m.get("inputFile");
    String outputFile = m.get("outputFile");
    String tmpFile = inputFile + ".tmp";

    LOG.info("Building Docker tasks");
    Task stepOne =
        TaskBuilder.named("TaskOne")
            .project(m.get("project"))
            .logging(m.get("logging") + "/1/test.log")
            .zones(new String[] {"us-*"})
            .inputFile("inputFile", inputFile)
            .input("message", "hello")
            .outputFile("outputFile", tmpFile)
            .docker("ubuntu")
            .script("cp ${inputFile} ${outputFile} ; echo ${message} >> ${outputFile}")
            .build();

    Task stepTwo =
        TaskBuilder.named("TaskTwo")
            .project(m.get("project"))
            .logging(m.get("logging") + "/2/test.log")
            .zones(new String[] {"us-*"})
            .inputFile("inputFile", tmpFile)
            .input("message", "goodbye")
            .outputFile("outputFile", outputFile)
            .docker("ubuntu")
            .script("cp ${inputFile} ${outputFile} ; echo ${message} >> ${outputFile}")
            .build();

    LOG.info("Defining and running Dataflow pipeline");
    Workflow w =
        TaskBuilder.named(LinearGraph.class.getSimpleName()).steps(stepOne, stepTwo).build();
    DataflowBuilder.of(w).createFrom(args).pipelineOptions(args).build().run();
  }
}
