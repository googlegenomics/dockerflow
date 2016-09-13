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
  String HELP = "help";
  String WORKFLOW_FILE = "workflow-file";
  String WORKFLOW_CLASS = "workflow-class";
  String TASK_FILE = "task-file";
  String LOGGING = "logging";
  String WORKSPACE = "workspace";
  String PROJECT = "project";
  String INPUTS = "inputs";
  String INPUTS_FROM_FILE = "inputs-fron-file";
  String OUTPUTS = "outputs";
  String ARGS_FILE = "args-file";
  String GLOBALS = "globals";
  String ZONES = "zones";
  String DISK_SIZE = "disk-size";
  String CPU = "cpu";
  String MEMORY = "memory";
  String PREEMPTIBLE = "preemptible";
  String RUN_ID = "run-id";
  String SERVICE_ACCOUNT_NAME = "service-account-name";
  String SERVICE_ACCOUNT_SCOPES = "service-account-scopes";
  String KEEP_ALIVE = "keep-alive";
  String TEST = "test";
  String RESUME = "resume";
  String DELETE_FILES = "delete-files";
  String ABORT = "abort";
  String STAGING = "staging";
  String STAGING_LOCATION = "stagingLocation";
  String RUNNER = "runner";
  String MAX_WORKERS = "max-workers";
  String MACHINE_TYPE = "machine-type";
  String DEFAULT_MACHINE_TYPE = "n1-standard-1";
  String DIRECT_RUNNER = DirectPipelineRunner.class.getSimpleName();
  String BLOCKING_RUNNER = BlockingDataflowPipelineRunner.class.getSimpleName();
  String DEFAULT_RUNNER = DataflowPipelineRunner.class.getSimpleName();
  int POLL_INTERVAL = 30;
  String WILDCARD = "*";
  String API_RUN_PIPELINE = "https://genomics.googleapis.com/v1alpha2/pipelines:run";
  String API_OPERATIONS = "https://genomics.googleapis.com/v1alpha2/";
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
  String PREFIX_INPUT = "<";
  String PREFIX_OUTPUT = ">";
  String DEFAULT_DISK_NAME = "data";
  String DEFAULT_MOUNT_POINT = "/mnt/data";
  String DEFAULT_DISK_TYPE = "PERSISTENT_HDD";
  String DEFAULT_DISK_SIZE = "500";
  int DEFAULT_MAX_TRIES = 3;
  String MAX_TRIES = "max-tries";
  /** The key in the workflow graph definition to indicate a branch in the graph. */
  String BRANCH = "BRANCH";
}
