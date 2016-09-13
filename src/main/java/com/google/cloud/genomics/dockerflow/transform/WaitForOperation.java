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
package com.google.cloud.genomics.dockerflow.transform;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.runner.Operation;
import com.google.cloud.genomics.dockerflow.runner.TaskException;
import com.google.cloud.genomics.dockerflow.runner.TaskRunner;
import com.google.cloud.genomics.dockerflow.task.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Poll for task completion. If testing, return immediately. */
@SuppressWarnings("serial")
public class WaitForOperation extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {
  private static final Logger LOG = LoggerFactory.getLogger(WaitForOperation.class);
  private Task task;

  public WaitForOperation(Task t) {
    task = t;
  }

  @Override
  public void processElement(
      DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c) throws Exception {
    WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
    Operation o = wa.getCurrentOperation();

    // Task is already done.
    if (o != null && o.getDone()) {
      c.output(c.element());
    }
    // Wait for it
    else {
      if (wa.isTesting() != null && wa.isTesting()) {
        LOG.info("Running in local/test mode. Not waiting for the operation to complete.");
      } else {
        LOG.info("Waiting for " + o.getName());
        o = TaskRunner.wait(wa.getCurrentOperation());
      }
      wa.setCurrentOperation(o);

      LOG.info("Operation name: " + o.getName() + " completed.");

      // Check for errors and abort if that's the policy
      if (o.getError() != null
          && task.getArgs() instanceof WorkflowArgs
          && ((WorkflowArgs) task.getArgs()).getAbortOnError()) {
        String msg = o.getError().getMessage();
        if (o.getError().getDetails() != null) {
          msg += ". " + o.getError().getDetails();
        }
        // Don't abort if it was due to VM preemption
        if (msg.indexOf("stopped unexpectedly") < 0) {
          throw new TaskException("Operation " + o.getName() + " failed. Details: " + msg);
        } else {
          LOG.info(
              "VM was preempted. Task will be retried up to " 
              + wa.getMaxTries()
              + " attempts.");
        }
      }

      c.output(KV.of(c.element().getKey(), wa));
    }
  }
}
