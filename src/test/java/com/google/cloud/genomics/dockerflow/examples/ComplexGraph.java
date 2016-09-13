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
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Branch;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Steps;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a complex workflow graph with Docker steps using Dataflow for orchestration.
 *
 * <p>Required command-line arguments:
 *
 * <pre>
 * --project=YOUR_PROJECT_ID
 * --workspace=gs://YOUR_BUCKET/DIR
 * --runner=DATAFLOW_RUNNER_NAME
 * --max-workers=INT (recommended=3)
 * </pre>
 */
public class ComplexGraph {
  private static final Logger LOG = LoggerFactory.getLogger(ComplexGraph.class);

  public static void main(String[] args) throws IOException {
    LOG.info("Defining and running Dataflow pipeline");
    Workflow w =
        TaskBuilder.named(ComplexGraph.class.getSimpleName())
            .steps(
                Steps.of(
                    task("one"),
                    Branch.of(
                        Branch.of(Steps.of(task("two"), task("three")), task("four")),
                        Steps.of(task("five"), task("six"), task("seven")),
                        task("eight"))))
            .build();
    DataflowBuilder.of(w).createFrom(args).pipelineOptions(args).build().run();
  }

  public static Task task(String name) throws IOException {
    LOG.info("Building Docker task: " + name);
    return TaskBuilder.named(name)
        .input("name", name)
        .docker("ubuntu")
        .script("echo Task=${name}")
        .build();
  }
}
