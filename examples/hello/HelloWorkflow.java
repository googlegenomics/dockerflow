/*
 * Copyright 2016 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowDefn;

/**
 * A Hello, World example in Java.
 */
public class HelloWorkflow implements WorkflowDefn {

  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    Task hello =
        TaskBuilder.named("Hello").input("message").script("echo $message").build();
    return TaskBuilder.named("HelloWorkflow").steps(hello).args(args).build();
  }
}