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

import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.args.ArgsBuilder;
import com.google.cloud.genomics.dockerflow.args.TaskArgs;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.task.TaskDefn;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Disk;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Resources;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for Tasks and Workflows.
 */
public class WorkflowFactory implements DockerflowConstants {
  static final Logger LOG = LoggerFactory.getLogger(WorkflowFactory.class);

  /**
   * Load a task definition (the Pipelines API yaml/json).
   *
   * @param path local or GCS path, yaml or json
   * @return
   * @throws IOException
   */
  public static TaskDefn loadDefn(String path) throws IOException {
    return FileUtils.parseFile(path, TaskDefn.class);
  }

  /**
   * Load a workflow definition (the Pipelines API yaml/json).
   *
   * @param path local or GCS path, yaml or json
   * @return
   * @throws IOException
   */
  public static Workflow loadWorkflowDefn(String path) throws IOException {
    return FileUtils.parseFile(path, Workflow.class);
  }

  /**
   * Load a workflow definition.
   *
   * @param path local or GCS path, yaml or json
   * @throws URISyntaxException
   */
  public static Workflow load(String path) throws IOException, URISyntaxException {
    LOG.info("Load workflow: " + path);

    Workflow w = loadWorkflowDefn(path);
    TaskArgs a = w.getArgs();
    w.setArgs(new WorkflowArgs(a));
    
    // Expand zone wildcards
    if (w.getArgs() != null && w.getArgs().getResources() != null && w.getArgs().getResources().getZones() != null) {
      String[] zones =
          a.getResources().getZones().toArray(new String[a.getResources().getZones().size()]);
      a.getResources().setZones(WorkflowFactory.expandZones(zones));
    }

    // Now load any child pipeline definitions and merge into the graph
    for (Workflow step : w.getSteps()) {

      // Inject a task from file
      if (step.getDefnFile() != null) {
        String tfile = FileUtils.resolve(step.getDefnFile(), path);
        TaskDefn defn = loadDefn(tfile);
        step.setDefnFile(tfile);

        // Override the name in the file if one is provided higher up in
        // the hierarchy
        if (step.getDefn() != null && step.getDefn().getName() != null) {
          defn.setName(step.getDefn().getName());
        }
        step.setDefn(defn);

      // Inject a workflow from file
      } else if (step.getWorkflowDefnFile() != null) {
        String wfile = FileUtils.resolve(step.getWorkflowDefnFile(), path);
        step.setWorkflowDefnFile(wfile);
        LOG.info("Load nested workflow file: " + wfile);

        Workflow child = load(wfile);

        // Override the name in the file if one is provided higher up in
        // the hierarchy
        if (step.getDefn().getName() != null) {
          child.getDefn().setName(step.getDefn().getName());
        }

        // Check errors: if loading from file, the parent cannot define
        // pipeline args, graph or stages
        if (step.getArgs() != null) {
          throw new IOException(
              "Workflow stage "
                  + step.getDefn().getName()
                  + " in file "
                  + path
                  + " cannot include \"args\" if "
                  + WORKFLOW_FILE
                  + " is set.");
        }
        if (step.getGraph() != null && !step.getGraph().isEmpty()) {
          throw new IOException(
              "Workflow stage "
                  + step.getDefn().getName()
                  + " in file "
                  + path
                  + " cannot include \"graph\" if "
                  + WORKFLOW_FILE
                  + " is set.");
        }
        if (step.getSteps() != null && !step.getSteps().isEmpty()) {
          throw new IOException(
              "Workflow stage "
                  + step.getDefn().getName()
                  + " in file "
                  + path
                  + " cannot include \"steps\" if "
                  + WORKFLOW_FILE
                  + " is set.");
        }

        // Copy info from the file
        step.setDefn(child.getDefn());
        step.setArgs(child.getArgs());
        w.getArgs().mergeDefaultArgs(child.getArgs());
        step.setGraph(child.getGraph());
        step.setSteps(child.getSteps());
      }

      // Add a default disk if none was specified in the file
      if (step.getDefn().getResources() == null) {
        step.getDefn().setResources(new Resources());
      }
      if (step.getDefn().getResources().getDisks() == null
          || step.getDefn().getResources().getDisks().isEmpty()) {
        step.getDefn().getResources().setDisks(new ArrayList<Disk>());
        step.getDefn().getResources().getDisks().add(Disk.defaultDisk());
      }
    }
    return w;
  }

  /**
   * Parse command-line options to create a datapipe.
   *
   * @param args from command-line
   * @return a pipeline request
   * @throws IOException
   * @throws URISyntaxException
   */
  public static Workflow create(String[] args) throws IOException, URISyntaxException {
    Map<String, String> m = StringUtils.parseArgs(args);
    Workflow w;

    // Load workflow from file
    if (m.containsKey(WORKFLOW_FILE)) {
      w = TaskBuilder.fromFile(m.get(WORKFLOW_FILE)).build();
    // Load a single task workflow
    } else if (m.containsKey(TASK_FILE)) {
      w = TaskBuilder.fromTaskDefnFile(m.get(TASK_FILE)).build();
    } else {
      throw new IllegalArgumentException(
          "One of "
              + WORKFLOW_FILE
              + ", "
              + TASK_FILE
              + ", and "
              + WORKFLOW_CLASS
              + " is required");
    }
    return w;
  }

  public static WorkflowArgs createArgs(String[] args) throws IOException {
    Map<String, String> m = StringUtils.parseArgs(args);
    ArgsBuilder b = ArgsBuilder.of();

    // Set all other parameters
    if (m.containsKey(WORKSPACE)) {
      b.workspace(m.get(WORKSPACE));
    }
    if (m.containsKey(LOGGING)) {
      b.logging(m.get(LOGGING));
    } else if (m.containsKey(WORKSPACE)) {
      b.logging(m.get(WORKSPACE) + "/logs");
    }
    if (m.containsKey(PROJECT)) {
      b.project(m.get(PROJECT));
    }
    if (m.containsKey(INPUTS)) {
      Map<String, String> kv = StringUtils.parseParameters(m.get(INPUTS), false);
      for (String k : kv.keySet()) {
        b.input(k, kv.get(k));
      }
    }
    if (m.containsKey(INPUTS_FROM_FILE)) {
      Map<String, String> kv = StringUtils.parseParameters(m.get(INPUTS_FROM_FILE), true);
      for (String k : kv.keySet()) {
        b.input(k, kv.get(k));
      }
    }
    if (m.containsKey(OUTPUTS)) {
      Map<String, String> kv = StringUtils.parseParameters(m.get(OUTPUTS), false);
      for (String k : kv.keySet()) {
        b.output(k, kv.get(k));
      }
    }
    if (m.containsKey(ZONES)) {
      b.zones(m.get(ZONES).split(","));
    }
    if (m.containsKey(DISK_SIZE)) {
      b.diskSize(Integer.parseInt(m.get(DISK_SIZE)));
    }
    if (m.containsKey(CPU)) {
      b.cpu(Integer.parseInt(m.get(CPU)));
    }
    if (m.containsKey(MEMORY)) {
      b.memory(Double.parseDouble(m.get(MEMORY)));
    }
    if (m.containsKey(PREEMPTIBLE)) {
      b.preemptible(Boolean.parseBoolean(m.get(PREEMPTIBLE)));
    }
    if (m.containsKey(RUN_ID)) {
      b.clientId(m.get(RUN_ID));
    }
    if (m.containsKey(SERVICE_ACCOUNT_NAME)) {
      b.serviceAccountEmail(m.get(SERVICE_ACCOUNT_NAME));
    }
    if (m.containsKey(SERVICE_ACCOUNT_SCOPES)) {
      b.serviceAccountScopes(Arrays.asList(m.get(SERVICE_ACCOUNT_SCOPES).split(",")));
    }
    if (m.containsKey(KEEP_ALIVE)) {
      b.keepAlive(m.get(KEEP_ALIVE));
    }
    if (m.containsKey(TEST)) {
      b.testing(Boolean.valueOf(m.get(TEST)));
    }
    if (m.containsKey(RESUME)) {
      b.resumeFailedRun(Boolean.valueOf(m.get(RESUME)));
    }
    if (m.containsKey(DELETE_FILES)) {
      b.deleteIntermediateFiles(Boolean.valueOf(m.get(DELETE_FILES)));
    }
    if (m.containsKey(ABORT)) {
      b.abortOnError(Boolean.valueOf(m.get(ABORT)));
    }
    if (m.containsKey(MAX_TRIES)) {
      b.maxTries(Integer.valueOf(m.get(MAX_TRIES)));
    }

    WorkflowArgs wa = b.build();

    if (wa.getProjectId() == null) {
      throw new IllegalArgumentException("--" + PROJECT + " is required");
    }
    if (wa.getLogging().getGcsPath() == null) {
      throw new IllegalArgumentException("--" + LOGGING + " is required");
    }

    return wa;
  }

  /** Expand wildcards for zones, like us-central* or eu*, to include the list of matching zones. */
  public static List<String> expandZones(String[] zones) {
    List<String> expanded = new ArrayList<String>();
    for (String s : zones) {
      if (s.endsWith(WILDCARD)) {
        s = s.substring(0, s.length() - 1);
        for (String z : ALL_ZONES) {
          if (z.startsWith(s)) {
            expanded.add(z);
          }
        }
      } else {
        expanded.add(s);
      }
    }
    return expanded;
  }
}
