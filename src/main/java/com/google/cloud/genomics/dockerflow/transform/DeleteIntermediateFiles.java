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
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;
import com.google.cloud.genomics.dockerflow.util.HttpUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keep only the files named explicitly as outputs of the workflow. */
@SuppressWarnings("serial")
public class DeleteIntermediateFiles
    extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteIntermediateFiles.class);
  private Task task;

  public DeleteIntermediateFiles(Task t) {
    this.task = t;
  }

  @Override
  public void processElement(
      DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c) {
    LOG.info("Deleting intermediate files");

    String key = c.element().getKey();
    WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
    try {
      LOG.info(StringUtils.toJson(wa));
    } catch (Exception e) {
    }
    Task t = new Task(task);
    t.substitute(wa.getInputs());
    t.substitute(wa.getOutputs());

    LOG.info("Finding files to keep");
    Map<String, String> toRetain = new LinkedHashMap<String, String>();
    if (t.getArgs() != null && t.getArgs().getOutputs() != null) {
      toRetain.putAll(t.getArgs().getOutputs());
    }
    try {
      LOG.info("Files to keep:\n" + StringUtils.toJson(toRetain));
    } catch (IOException e) {
    }

    LOG.info("Finding intermediate files to delete");
    Map<String, String> toDelete = new LinkedHashMap<String, String>();
    if (wa.getOutputs() != null) {
      toDelete.putAll(wa.getOutputs());
    }
    for (String name : toRetain.keySet()) {
      toDelete.remove(name);
    }

    for (String pathToKeep : toRetain.values()) {
      for (String name : new HashSet<String>(toDelete.keySet())) {

        // Check if the same path has different var names
        if (pathToKeep.equals(toDelete.get(name))) {
          toDelete.remove(name);
        }
      }
    }

    try {
      LOG.info("Files to delete:\n" + StringUtils.toJson(toDelete));
    } catch (IOException e) {
    }

    LOG.info("Deleting intermediate files");
    for (String name : toDelete.keySet()) {

      LOG.debug("Deleting: " + name + ": " + wa.getOutputs().get(name));

      // There may be multiple delimited paths
      String val = wa.getOutputs().get(name);
      String[] paths;
      if (val != null && val.trim().length() != 0) {
        paths = wa.getOutputs().get(name).split(Param.ARRAY_DELIMITER_REGEX);
      } else {
        paths = new String[] {val};
      }

      // Delete each
      for (String path : paths) {
        try {
          LOG.debug("Deleting file: " + path);
          String result = HttpUtils.doDelete(path);
          LOG.debug(result);

          // Remove from args available to downstream steps
          wa.getOutputs().remove(name);

        } catch (IOException e) {
          LOG.debug("Failed to delete file for output: " + name + ". Reason: " + e.getMessage());
        }
      }
    }

    LOG.info("Finished retaining outputs");
    c.output(KV.of(key, wa));
  }
}
