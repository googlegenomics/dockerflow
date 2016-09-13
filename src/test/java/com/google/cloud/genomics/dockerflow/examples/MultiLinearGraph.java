/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dockerflow.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.genomics.dockerflow.args.ArgsTableBuilder;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowBuilder;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowFactory;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.transform.DockerDo;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run two instances of a simple workflow, each with different inputs and outputs. All workflow
 * steps run in Docker using Dataflow for orchestration.
 *
 * <p>Required command-line arguments:
 *
 * <pre>
 * --project=YOUR_PROJECT_ID
 * --workspace=gs://YOUR_BUCKET/DIR
 * --args-table=CSV_FILE
 * --runner=DATAFLOW_RUNNER_NAME
 * </pre>
 */
public class MultiLinearGraph {
  private static final Logger LOG = LoggerFactory.getLogger(MultiLinearGraph.class);

  /**
   * Run the example using the FlowBuilder class to construct the workflow graph and Dataflow
   * pipeline automatically. You can use it even for arbitrarily complex directed acyclic graphs.
   */
  public static void main(String[] args) throws IOException {
    LOG.info("Defining and running Dataflow pipeline");
    Workflow w =
        TaskBuilder.named(MultiLinearGraph.class.getSimpleName())
            .steps(taskOne(), taskTwo())
            .build();
    DataflowBuilder.of(w).createFrom(args).pipelineOptions(args).build().run();
  }

  /**
   * For simple linear graphs, it's not too hard to generate the Dataflow pipeline yourself. Here's
   * the equivalent Dataflow code for this simple example.
   */
  public static void manualDataflow(String[] args) throws IOException {
    LOG.info("Parsing Dataflow options");
    DataflowPipelineOptions o = DataflowFactory.pipelineOptions(args);
    o.setAppName(MultiLinearGraph.class.getSimpleName());
    Pipeline p = Pipeline.create(o);

    p.apply(Create.of(ArgsTableBuilder.fromArgs(args).build()))
        .apply(DockerDo.of(taskOne()))
        .apply(DockerDo.of(taskTwo()));
    p.run();
  }

  public static Task taskOne() throws IOException {
    LOG.info("Building Docker task: TaskOne.");
    return TaskBuilder.named("TaskOne")
        .inputFile("inputFile")
        .input("message", "hello")
        .outputFile("outputFile")
        .docker("ubuntu")
        .script("cp ${inputFile} ${outputFile} ; echo ${message} >> ${outputFile}")
        .build();
  }

  public static Task taskTwo() throws IOException {
    LOG.info("Building Docker task: TaskTwo.");
    return TaskBuilder.named("TaskTwo")
        .inputFile("inputFile")
        .input("message", "hello")
        .outputFile("outputFile")
        .docker("ubuntu")
        .script("cp ${inputFile} ${outputFile} ; echo ${message} >> ${outputFile}")
        .build();
  }
}
