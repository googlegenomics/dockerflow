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
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.genomics.dockerflow.args.ArgsTableBuilder;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowBuilder;
import com.google.cloud.genomics.dockerflow.dataflow.DataflowFactory;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;

import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line submitter for single tasks to be run in the cloud.
 */
public class Dsub implements DockerflowConstants {
  private static final Logger LOG = LoggerFactory.getLogger(Dsub.class);

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
          "Description:\n"
              + "  Submit a task to run in Google Cloud.\n\n"
              + "COMMON OPTIONS:\n"
              + "--" + NAME + "=STRING\n"
              + "  Name of the task. Must be unique within the pipeline.\n"
              + "--" + SCRIPT_FILE + "=PATH\n"
              + "  Local or GCS file containing the shell commands to run.\n"
              + "--" + ARGS_FILE + "=PATH\n"
              + "  Workflow args in yaml/json in GCS or local. Or a csv with one run per\n"
              + "  row and param names in columns.\n"
              + "--" + INPUTS + "=KEY=VAL,KEY2=VAL2\n"
              + "  Input parameters to the pipeline.\n"
              + "--" + INPUT_FILE + "=KEY=GCS-PATH\n"
              + "  An input file in GCS that will be copied to local disk at a path\n"
              + "  that can be referenced within the script as ${KEY}.\n"
              + "--" + OUTPUTS + "=KEY=GCS-PATH\n"
              + "  An output file from the task that will be copied from local disk\n"
              + "  to GCS and can be referenced with the script as ${KEY}.\n"
              + "--" + PROJECT + "=PROJECT_ID\n"
              + "  REQUIRED. Google Cloud Project name.\n"
              + "--" + TEST + "=BOOL\n"
              + "  Dry run for testing. Docker tasks will not execute.\n"
              + "--" + WORKSPACE + "=PATH\n"
              + "  Base path in GCS for input, output, and logging files.\n"
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
              + "--" + INPUTS_FROM_FILE + "=KEY=PATH\n"
              + "  Load parameter values from a local or GCS file.\n"
              + "--" + KEEP_ALIVE + "=INT\n"
              + "  Seconds to keep VMs alive after failure to ssh in and debug.\n"
              + "--" + LOGGING + "=PATH\n"
              + "  Base GCS folder where logs will be written.\n"
              + "--" + MAX_TRIES + "=INT\n"
              + "  Maximum preemptible tries. Default: " + DEFAULT_MAX_TRIES
              + "--" + MEMORY + "=INT\n"
              + "  Override minimum memory in GB.\n"
              + "--" + PREEMPTIBLE + "=BOOL\n"
              + "  Run with preemptible VMs if the pipeline supports it.\n"
              + "--" + RUN_ID + "=STRING\n"
              + "  A name for the task that will be added to operations as a label.\n"
              + "--" + SCATTER + "=INPUT_NAME"
              + "  Run multiple task instances, one per value of INPUT_NAME.\n"
              + "--" + SCRIPT + "=STRING\n"
              + "  A shell command to run.\n"
          );
      System.out.println("OAuth token: " 
          + GoogleCredential.getApplicationDefault().getAccessToken() + "\n");
      return;
    }

    Workflow w = WorkflowFactory.createTask(args);
    Map<String, WorkflowArgs> argsTable = ArgsTableBuilder.fromArgs(args).build();

    DataflowPipelineOptions pipelineOptions = DataflowFactory.pipelineOptions(args);
    pipelineOptions.setRunner(DirectPipelineRunner.class);
    Pipeline dataflow =
        DataflowBuilder.of(w)
            .createFrom(argsTable)
            .pipelineOptions(pipelineOptions)
            .build();
    
    LOG.info(
        "Running task "
            + ((DataflowPipelineOptions) dataflow.getOptions()).getAppName()
            + "\nTo check status, run:\n"
            + "> gcloud alpha genomics operations describe OPERATION_ID"
            + "\nTo view logs and outputs, run:\n"
            + "> gsutil ls " + w.getArgs().getLogging().getGcsPath()
            + "\nTo cancel, run:\n"
            + "> gcloud alpha genomics operations cancel OPERATION_ID");

    PipelineResult result = dataflow.run();

    LOG.info("State: " + result.getState());
  }
}
