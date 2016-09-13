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

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.runner.Operation;

/**
 * Arguments for running an entire workflow. Individual parameters will flow down to the subtasks.
 */
@DefaultCoder(SerializableCoder.class)
@SuppressWarnings("serial")
public class WorkflowArgs extends TaskArgs {
  private Operation currentOperation;

  // To resolve any relative paths.
  private String basePath;

  // When there are multiple concurrent instances of the workflow running
  private int runIndex;

  // Add retries, such as for preemption
  private int maxTries = DockerflowConstants.DEFAULT_MAX_TRIES;

  private Boolean isTesting;

  // Abort on error; otherwise carry on with the workflow provided
  // subsequent steps are fault-tolerant
  private boolean abortOnError = true;

  // Delete intermediate files
  private boolean deleteIntermediateFiles;

  // If output files exist, attempt to resume from the last completed step.
  private boolean resumeFailedRun;

  public WorkflowArgs() {
    super();
  }

  /** Copy constructor. */
  public WorkflowArgs(TaskArgs ta) {
    super(ta);

    if (ta instanceof WorkflowArgs) {
      WorkflowArgs wa = (WorkflowArgs) ta;
      currentOperation = wa.currentOperation;
      basePath = wa.basePath;
      runIndex = wa.runIndex;
      maxTries = wa.maxTries;
      isTesting = wa.isTesting;
      abortOnError = wa.abortOnError;
      deleteIntermediateFiles = wa.deleteIntermediateFiles;
      resumeFailedRun = wa.resumeFailedRun;
    }
  }

  @Override
  public void applyArgs(TaskArgs args) {
    super.applyArgs(args);

    if (args instanceof WorkflowArgs) {
      WorkflowArgs wa = (WorkflowArgs) args;
      basePath = wa.basePath;
      isTesting = wa.isTesting;
      abortOnError = wa.abortOnError;
      resumeFailedRun = wa.resumeFailedRun;
      deleteIntermediateFiles = wa.deleteIntermediateFiles;
    }
  }

  @Override
  public void mergeDefaultArgs(TaskArgs defaultArgs) {
    super.mergeDefaultArgs(defaultArgs);

    if (defaultArgs instanceof WorkflowArgs) {
      WorkflowArgs wa = (WorkflowArgs) defaultArgs;

      if (basePath == null) {
        basePath = wa.getBasePath();
      }
      if (isTesting == null) {
        isTesting = wa.isTesting();
      }
    }
  }

  /**
   * The name of the currently running operation. The value is set internally when a Docker task
   * starts and is nulled out when the task completes.
   */
  public Operation getCurrentOperation() {
    return currentOperation;
  }

  public void setCurrentOperation(Operation operation) {
    this.currentOperation = operation;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public int getRunIndex() {
    return runIndex;
  }

  public void setRunIndex(int index) {
    this.runIndex = index;
  }

  public int getMaxTries() {
    return maxTries;
  }

  public void setMaxTries(int tries) {
    if (tries < 1) {
      throw new IllegalArgumentException("Max tries must be at least one");
    }
    this.maxTries = tries;
  }

  public Boolean isTesting() {
    return isTesting;
  }

  public void setTesting(Boolean isTesting) {
    this.isTesting = isTesting;
  }

  public boolean getAbortOnError() {
    return abortOnError;
  }

  public void setAbortOnError(boolean abortOnError) {
    this.abortOnError = abortOnError;
  }

  public boolean getDeleteFiles() {
    return deleteIntermediateFiles;
  }

  public void setDeleteFiles(boolean deleteFiles) {
    this.deleteIntermediateFiles = deleteFiles;
  }

  public boolean getResumeFailedRun() {
    return resumeFailedRun;
  }

  public void setResumeFailedRun(boolean resume) {
    this.resumeFailedRun = resume;
  }
}
