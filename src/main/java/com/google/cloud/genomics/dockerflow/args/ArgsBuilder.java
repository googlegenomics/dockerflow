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
package com.google.cloud.genomics.dockerflow.args;

import com.google.cloud.genomics.dockerflow.args.TaskArgs.Logging;
import com.google.cloud.genomics.dockerflow.args.TaskArgs.ServiceAccount;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Disk;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Resources;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builder for workflow arguments. */
public class ArgsBuilder {
  private WorkflowArgs workflowArgs;

  public static ArgsBuilder of() {
    return new ArgsBuilder(null);
  }

  /**
   * Constructor.
   *
   * @param clientId a name for the overall workflow instance
   * @return
   */
  public static ArgsBuilder of(String clientId) {
    return new ArgsBuilder(clientId);
  }

  /** Load the workflow args from yaml / json from a local or GCS file. */
  public static ArgsBuilder fromFile(String path) throws IOException {
    ArgsBuilder b = new ArgsBuilder(null);
    b.workflowArgs = FileUtils.parseFile(path, WorkflowArgs.class);
    return b;
  }

  ArgsBuilder(String clientId) {
    workflowArgs = new WorkflowArgs();
    if (clientId != null) {
      workflowArgs.setClientId(clientId); // keep any default
    }
  }

  public static ArgsBuilder fromArgs(String[] args) throws IOException {
    return fromArgs(args, null);
  }

  public static ArgsBuilder fromArgs(String[] args, String name) throws IOException {
    ArgsBuilder b = ArgsBuilder.of(name);
    b.workflowArgs = WorkflowFactory.createArgs(args);
    return b;
  }

  public ArgsBuilder inputs(Map<String, String> inputs) {
    if (workflowArgs.getInputs() == null) {
      workflowArgs.setInputs(new LinkedHashMap<String, String>());
    }
    workflowArgs.getInputs().putAll(inputs);
    return this;
  }

  public ArgsBuilder input(String name, String value) {
    workflowArgs.set(name, value);
    return this;
  }

  public ArgsBuilder inputFromFile(String name, String value) {
    workflowArgs.set(name, value);
    workflowArgs.setFromFile(name, true);
    return this;
  }

  public ArgsBuilder outputs(Map<String, String> outputs) {
    if (workflowArgs.getOutputs() == null) {
      workflowArgs.setOutputs(new LinkedHashMap<String, String>());
    }
    workflowArgs.getInputs().putAll(outputs);
    return this;
  }

  public ArgsBuilder output(String name, String value) {
    if (workflowArgs.getOutputs() == null) {
      workflowArgs.setOutputs(new LinkedHashMap<String, String>());
    }
    workflowArgs.getOutputs().put(name, value);
    return this;
  }

  public ArgsBuilder cpu(int cores) {
    workflowArgs.getResources().setMinimumCpuCores(String.valueOf(cores));
    return this;
  }

  public ArgsBuilder memory(double gb) {
    if (workflowArgs.getResources() == null) {
      workflowArgs.setResources(new Resources());
    }
    workflowArgs.getResources().setMinimumRamGb(String.valueOf(gb));
    return this;
  }

  public ArgsBuilder diskSize(int gb) {
    if (workflowArgs.getResources() == null) {
      workflowArgs.setResources(new Resources());
    }
    if (workflowArgs.getResources().getDisks() == null) {
      workflowArgs.getResources().setDisks(new ArrayList<Disk>());
      workflowArgs.getResources().getDisks().add(new Disk());
    }
    workflowArgs.getResources().getDisks().get(0).setSizeGb(String.valueOf(gb));
    return this;
  }

  public ArgsBuilder preemptible(boolean b) {
    if (workflowArgs.getResources() == null) {
      workflowArgs.setResources(new Resources());
    }
    workflowArgs.getResources().setPreemptible(b);
    return this;
  }

  public ArgsBuilder zones(String[] zones) {
    if (workflowArgs.getResources() == null) {
      workflowArgs.setResources(new Resources());
    }
    workflowArgs.getResources().setZones(WorkflowFactory.expandZones(zones));
    return this;
  }

  public ArgsBuilder project(String id) {
    workflowArgs.setProjectId(id);
    return this;
  }

  /**
   * The base logging path. All task logs will be stored in sub-folders. The log files will be named
   * task.log, task-stdout.log, and task-stderr.log.
   */
  public ArgsBuilder logging(String path) {
    return logging(path, false);
  }

  /**
   * The base logging path. All task logs will be stored in sub-folders, defined as
   * ${workflow.element}. The log files will be named [operationId].log, [operationId]-stdout.log,
   * and [operationId]-stderr.log.
   */
  public ArgsBuilder logging(String path, boolean useOperationName) {
    String s = path + "/${" + Task.WORKFLOW_ELEMENT + "}";
    if (!useOperationName) {
      s += "/" + Task.TASK_LOG;
    }

    workflowArgs.setLogging(new Logging());
    workflowArgs.getLogging().setGcsPath(s);
    return this;
  }

  /**
   * The base path for task output files, if they use relative paths. If so, all outputs will be
   * stored in sub-folders, defined as ${workflow.element}.
   *
   * @param path
   * @return
   */
  public ArgsBuilder basePath(String path) {
    workflowArgs.setBasePath(
        path + (path.endsWith("/") ? "" : "/") + "${" + Task.WORKFLOW_ELEMENT + "}/");
    return this;
  }

  public ArgsBuilder clientId(String id) {
    workflowArgs.setClientId(id);
    return this;
  }

  public ArgsBuilder serviceAccountEmail(String email) {
    if (workflowArgs.getServiceAccount() == null) {
      workflowArgs.setServiceAccount(new ServiceAccount());
    }
    workflowArgs.getServiceAccount().setEmail(email);
    return this;
  }

  public ArgsBuilder serviceAccountScopes(List<String> scopes) {
    if (workflowArgs.getServiceAccount() == null) {
      workflowArgs.setServiceAccount(new ServiceAccount());
    }
    workflowArgs.getServiceAccount().setScopes(scopes);
    return this;
  }

  public ArgsBuilder keepAlive(String sec) {
    workflowArgs.setKeepVmAliveOnFailureDuration(sec);
    return this;
  }

  public ArgsBuilder testing(Boolean isTesting) {
    workflowArgs.setTesting(isTesting);
    return this;
  }

  public ArgsBuilder maxTries(int i) {
    workflowArgs.setMaxTries(i);
    return this;
  }

  public ArgsBuilder deleteIntermediateFiles(Boolean b) {
    workflowArgs.setDeleteFiles(b != null && b);
    return this;
  }

  public ArgsBuilder resumeFailedRun(Boolean b) {
    workflowArgs.setResumeFailedRun(b != null && b);
    return this;
  }

  public ArgsBuilder abortOnError(Boolean b) {
    workflowArgs.setAbortOnError(b != null && b);
    return this;
  }

  public WorkflowArgs build() {
    return workflowArgs;
  }
}
