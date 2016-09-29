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
package com.google.cloud.genomics.dockerflow;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.genomics.dockerflow.args.ArgsTableBuilder;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowBuilder;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowFactory;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowDefn;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line runner for Dataflow pipelines with shell steps running in Docker. Multi-step
 * pipelines are defined in yaml as a static graph. Command-line options can override default
 * settings provided in the graph. Individual Docker steps are described in separate yaml files.
 */
public class Dockerflow implements DockerflowConstants {
  private static final Logger LOG = LoggerFactory.getLogger(Dockerflow.class);

  /**
   * Run with --help for options.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    Map<String, String> m = StringUtils.parseArgs(args);

    // Show help and exit
    if (m.isEmpty() || m.containsKey(HELP)) {
      System.out.println(
          "USAGE: java "
              + Dockerflow.class.getName()
              + " [options]\n\n"
              + "Description:\n"
              + "  Run a workflow of Docker tasks defined in Java or yaml/json, using Dataflow "
              + "for orchestration.\n\n"
              + "COMMON OPTIONS:\n"
              + "--" + ARGS_FILE + "=PATH\n"
              + "  Workflow args in yaml/json in GCS or local. Or a csv with one run per "
              + "row and param names\n  in columns.\n"
              + "--" + INPUTS + "=KEY=VAL,KEY2=VAL2\n"
              + "  Input parameters to the pipeline.\n"
              + "--" + OUTPUTS + "=KEY=VAL,KEY2=VAL2\n"
              + "  Output files from the pipeline.\n"
              + "--" + PREEMPTIBLE + "=BOOL\n"
              + "  Run with preemptible VMs if the pipeline supports it.\n"
              + "--" + PROJECT + "=PROJECT_ID\n"
              + "  REQUIRED. Google Cloud Project name.\n"
              + "--" + RESUME + "=BOOL\n"
              + "  Attempt to resume a failed run. Useful when debugging\n"
              + "--" + RUNNER + "=DATAFLOW_RUNNER\n"
              + "  Default: " + DEFAULT_RUNNER
              + ". Use " + DIRECT_RUNNER + " for local testing.\n"
              + "--" + TEST + "=BOOL\n"
              + "  Dry run for testing. Docker tasks will not execute.\n"
              + "--" + WORKSPACE + "=PATH\n"
              + "  Base path for input, output, and logging files.\n"
              + "--" + WORKFLOW_CLASS + "=JAVA_CLASS\n"
              + "  A workflow defined in a Java class.\n"
              + "--" + WORKFLOW_FILE + "=PATH\n"
              + "  A workflow defined in yaml/json in GCS or local.\n"
              + "--" + ZONES + "=STRING\n"
              + "  Override zones for VMs. Wildcards like eu* are allowed.\n"
              + "\n"
              + "OTHER OPTIONS\n"
              + "--" + ABORT + "\n"
              + "  Abort if *any* concurrent task fails permanently. Otherwise, continue.\n"
              + "--" + CPU + "=INT\n"
              + "  Override minimum CPU cores.\n"
              + "--" + DISK_SIZE + "=INT\n"
              + "  Override size in Gb for all disks.\n"
              + "--" + GLOBALS + "=KEY=VAL,KEY2=VAL2\n"
              + "  Global parameters to substitute in the args-file.\n"
              + "--" + HELP 
              + "\n  Print this message.\n"
              + "--" + INPUTS_FROM_FILE + "=KEY=PATH,KEY2=PATH2\n"
              + "  Load parameter values from local files.\n"
              + "--" + KEEP_ALIVE + "=INT\n"
              + "  Seconds to keep VMs alive after failure to ssh in and debug.\n"
              + "--" + LOGGING + "=PATH\n"
              + "  Base GCS folder where logs will be written.\n"
              + "--" + MACHINE_TYPE + "=STRING\n"
              + "  Dataflow head node GCE instance type. Default: "
              + DEFAULT_MACHINE_TYPE
              + "--" + MAX_TRIES + "=INT\n"
              + "  Maximum preemptible tries. Default: " + DEFAULT_MAX_TRIES
              + "--" + MAX_WORKERS + "=INT\n"
              + "  Tip: set to the max number of parallel branches in the workflow.\n"
              + "--" + MEMORY + "=INT\n"
              + "  Override minimum memory in GB.\n"
              + "--" + RUN_ID + "=STRING\n"
              + "  An id provided by you to help operations to monitor or cancel.\n"
              + "--" + SERVICE_ACCOUNT_NAME + "=EMAIL\n"
              + "  Service account to use rather than the default GCE account.\n"
              + "--" + SERVICE_ACCOUNT_SCOPES + "=VAL,VAL2\n"
              + "  Service account scopes.\n"
              + "--" + STAGING + "=PATH\n"
              + "  Dataflow staging location for jars.\n"
              + "--" + TASK_FILE + "=PATH\n"
              + "  A single task defined in yaml/json in GCS or local.\n"
          );
      System.out.println("OAuth token: " 
          + GoogleCredential.getApplicationDefault().getAccessToken() + "\n");
      return;
    }
    LOG.info("Local working directory: " + new File(".").getAbsoluteFile());

    Map<String, WorkflowArgs> argsTable = ArgsTableBuilder.fromArgs(args).build();
    DataflowPipelineOptions pipelineOptions = DataflowFactory.pipelineOptions(args);
    Workflow w;
    Pipeline dataflow;
    
    if (m.containsKey(WORKFLOW_CLASS)) {
      LOG.info("Creating workflow from Java class " + m.get(WORKFLOW_CLASS));
      URLClassLoader cl =
          new URLClassLoader(new URL[] {new File(".").getAbsoluteFile().toURI().toURL()});
      WorkflowDefn d = (WorkflowDefn) cl.loadClass(m.get(WORKFLOW_CLASS)).newInstance();
      cl.close();
      w = d.createWorkflow(args);

    } else if (m.containsKey(WORKFLOW_FILE)) {
      LOG.info("Creating workflow from file " + m.get(WORKFLOW_FILE));
      w = WorkflowFactory.create(args);

    } else if (m.containsKey(TASK_FILE)) {
      LOG.info("Creating workflow from task file " + m.get(TASK_FILE));
      w = WorkflowFactory.create(args);

    } else {
      throw new IllegalArgumentException(
          "No workflow definition found. "
              + "Either a workflow-class, workflow-file, or task-file must be provided.");
    }

    dataflow =
        DataflowBuilder.of(w)
            .createFrom(argsTable)
            .pipelineOptions(pipelineOptions)
            .build();
    
    LOG.info(
        "Running Dataflow job "
            + ((DataflowPipelineOptions) dataflow.getOptions()).getAppName()
            + "\nTo cancel the individual Docker steps, run:\n"
            + "> gcloud alpha genomics operations cancel OPERATION_ID");

    PipelineResult result = dataflow.run();

    LOG.info("State: " + result.getState());
  }
}
