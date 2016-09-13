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
package com.google.cloud.genomics.dockerflow.workflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

/** A workflow definition. Dockerflow can instantiate workflows from the command-line. */
public interface WorkflowDefn {

  /**
   * The workflow defn implementation is responsible for defining the workflow steps and default
   * args, and creating a Dataflow pipeline.
   *
   * @throws URISyntaxException
   */
  Pipeline createDataflow(
      Map<String, WorkflowArgs> argsTable, DataflowPipelineOptions pipelineOptions, String[] args)
      throws IOException, URISyntaxException;
}
