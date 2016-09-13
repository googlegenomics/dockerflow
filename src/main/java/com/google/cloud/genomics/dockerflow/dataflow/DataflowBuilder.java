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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.genomics.dockerflow.args.ArgsTableBuilder;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import java.io.IOException;
import java.util.Map;

/**
 * Builder for workflow graphs.
 *
 * @author binghamj
 */
public class DataflowBuilder {
  private Workflow workflow;
  private Map<String, WorkflowArgs> workflowArgs;
  private DataflowPipelineOptions pipelineOptions;

  /**
   * Constructor.
   *
   * @param name
   * @return
   * @throws IOException
   */
  public static DataflowBuilder of(Workflow w) throws IOException {
    return new DataflowBuilder(w);
  }

  DataflowBuilder(Workflow w) throws IOException {
    workflow = w;
  }

  DataflowBuilder() {}

  /** Arguments for a single run. */
  public DataflowBuilder createFrom(WorkflowArgs args) {
    workflowArgs = ArgsTableBuilder.of(args).build();
    return this;
  }

  /** Arguments for multiple concurrent runs. */
  public DataflowBuilder createFrom(Map<String, WorkflowArgs> args) {
    workflowArgs = args;
    return this;
  }

  /** Arguments from the command line for one or more runs. */
  public DataflowBuilder createFrom(String[] args) throws IOException {
    workflowArgs = ArgsTableBuilder.fromArgs(args).build();
    return this;
  }

  public DataflowBuilder pipelineOptions(String[] args) throws IOException {
    return pipelineOptions(DataflowFactory.pipelineOptions(args));
  }

  public DataflowBuilder pipelineOptions(DataflowPipelineOptions options) {
    pipelineOptions = options;
    pipelineOptions.setAppName(workflow.getDefn().getName());
    return this;
  }

  public Pipeline build() throws IOException {
    return DataflowFactory.dataflow(workflow, workflowArgs, pipelineOptions);
  }
}
