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

import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;

/** Command-line flags and defaults. */
public interface DockerflowConstants {
  String ABORT = "abort";
  String[] ALL_ZONES = {
    "asia-east1-a",
    "asia-east1-b",
    "asia-east1-c",
    "europe-west1-b",
    "europe-west1-c",
    "europe-west1-d",
    "us-central1-a",
    "us-central1-b",
    "us-central1-c",
    "us-central1-f",
    "us-east1-b",
    "us-east1-c",
    "us-east1-d",
    "us-west1-a",
    "us-west1-b"
  };
  String API_OPERATIONS = "https://genomics.googleapis.com/v1alpha2/";
  String API_RUN_PIPELINE = "https://genomics.googleapis.com/v1alpha2/pipelines:run";
  String ARGS_FILE = "args-file";
  String BLOCKING_RUNNER = BlockingDataflowPipelineRunner.class.getSimpleName();
  /** The key in the workflow graph definition to indicate a branch in the graph. */
  String BRANCH = "BRANCH";
  String CPU = "cpu";
  String DEFAULT_DISK_NAME = "data";
  String DEFAULT_DISK_SIZE = "500";
  String DEFAULT_DISK_TYPE = "PERSISTENT_HDD";
  String DEFAULT_MACHINE_TYPE = "n1-standard-1";
  int DEFAULT_MAX_TRIES = 3;
  String DEFAULT_MOUNT_POINT = "/mnt/data";
  String DEFAULT_RUNNER = DataflowPipelineRunner.class.getSimpleName();
  String DELETE_FILES = "delete-files";
  String DIRECT_RUNNER = DirectPipelineRunner.class.getSimpleName();
  String DISK_SIZE = "disk-size";
  String GLOBALS = "globals";
  String HELP = "help";
  String INPUTS = "inputs";
  String INPUTS_FROM_FILE = "inputs-from-file";
  String KEEP_ALIVE = "keep-alive";
  String LOGGING = "logging";
  String MACHINE_TYPE = "machine-type";
  String MAX_TRIES = "max-tries";
  String MAX_WORKERS = "max-workers";
  String MEMORY = "memory";
  String OUTPUTS = "outputs";
  int POLL_INTERVAL = 30;
  String PREEMPTIBLE = "preemptible";
  String PREFIX_INPUT = "<";
  String PREFIX_OUTPUT = ">";
  String PROJECT = "project";
  String RESUME = "resume";
  String RUN_ID = "run-id";
  String RUNNER = "runner";
  String SERVICE_ACCOUNT_NAME = "service-account-name";
  String SERVICE_ACCOUNT_SCOPES = "service-account-scopes";
  String STAGING = "staging";
  String STAGING_LOCATION = "stagingLocation";
  String TASK_FILE = "task-file";
  String TEST = "test";
  String WILDCARD = "*";
  String WORKFLOW_CLASS = "workflow-class";
  String WORKFLOW_FILE = "workflow-file";
  String WORKSPACE = "workspace";
  String ZONES = "zones";
}
