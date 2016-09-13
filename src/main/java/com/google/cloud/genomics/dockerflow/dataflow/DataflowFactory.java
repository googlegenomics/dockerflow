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
package com.google.cloud.genomics.dockerflow.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.transform.DeleteIntermediateFiles;
import com.google.cloud.genomics.dockerflow.transform.DockerDo;
import com.google.cloud.genomics.dockerflow.transform.MergeBranches;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.GraphItem;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Branch;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Steps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory methods to create Dataflow Docker stuff. */
public class DataflowFactory implements DockerflowConstants {
  static final Logger LOG = LoggerFactory.getLogger(DataflowFactory.class);

  // Counter to disambiguate PTransform names in branched graphs
  private static int numMerges = 0;

  /**
   * Create Dataflow Pipeline options from the standard command-line options, "--project=",
   * "--runner=" and "--stagingLocation="
   *
   * @param args
   * @return
   * @throws IOException
   */
  public static DataflowPipelineOptions pipelineOptions(String[] args) throws IOException {
    LOG.info("Set up Dataflow options");
    DataflowPipelineOptions o = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    Map<String, String> m = StringUtils.parseArgs(args);
    o.setProject(m.get(PROJECT));
    if (m.containsKey(STAGING)) {
      o.setStagingLocation(m.get(STAGING));
    } else if (m.containsKey(STAGING_LOCATION)) {
      o.setStagingLocation(m.get(STAGING_LOCATION));
    } else if (m.containsKey(WORKSPACE)) {
      o.setStagingLocation(m.get(WORKSPACE) + "/staging");
    }
    o.setRunner(runner(m.get(RUNNER)));
    o.setMaxNumWorkers(m.get(MAX_WORKERS) == null ? 1 : Integer.parseInt(m.get(MAX_WORKERS)));
    if (m.containsKey(MACHINE_TYPE)) {
      o.setWorkerMachineType(m.get(MACHINE_TYPE));
    } else {
      o.setWorkerMachineType(DEFAULT_MACHINE_TYPE);
    }
    return o;
  }

  private static Class<? extends PipelineRunner<?>> runner(String name) {
    Class<? extends PipelineRunner<?>> c = DirectPipelineRunner.class; // default

    if (DEFAULT_RUNNER.equals(name) || name == null) {
      c = DataflowPipelineRunner.class;
    } else if (BLOCKING_RUNNER.equals(name)) {
      c = BlockingDataflowPipelineRunner.class;
    } else if (DIRECT_RUNNER.equals(name)) {
      c = DirectPipelineRunner.class;
    }
    return c;
  }

  /**
   * Dynamically construct a Dataflow from the workflow definition. The root PCollection has one
   * element, the root task's name.
   *
   * @param workflow
   * @param dataflowArgs
   * @return
   * @throws IOException
   */
  public static Pipeline dataflow(
      Workflow workflow, Map<String, WorkflowArgs> workflowArgs, DataflowPipelineOptions o)
      throws IOException {

    assert (workflow != null);
    assert (o != null);
    assert (workflow.getDefn() != null);

    // Set defaults
    if (o.getAppName() == null) {
      o.setAppName(workflow.getDefn().getName());
    }
    if (o.getProject() == null && workflow.getArgs() != null) {
      o.setProject(workflow.getArgs().getProjectId());
    }
    if (o.getMaxNumWorkers() == 0) {
      o.setMaxNumWorkers(1);
    }
    if (o.getWorkerMachineType() == null) {
      o.setWorkerMachineType(DEFAULT_MACHINE_TYPE);
    }

    LOG.info("Initializing dataflow pipeline");
    Pipeline p = Pipeline.create(o);

    LOG.info("Creating input collection of workflow args");
    if (workflowArgs == null) {
      workflowArgs = new HashMap<String, WorkflowArgs>();
    }
    if (workflowArgs.isEmpty()) {
      LOG.info("No workflow args were provided. Using default values.");
      workflowArgs.put(workflow.getDefn().getName(), new WorkflowArgs());
    } else if (workflow.getArgs() != null) {
      LOG.info("Merging default workflow args with instance-specific args");

      for (String key : workflowArgs.keySet()) {
        WorkflowArgs instanceArgs = workflowArgs.get(key);
        instanceArgs.mergeDefaultArgs(workflow.getArgs());
        LOG.debug("Merged args: " + StringUtils.toJson(instanceArgs));
      }
    }

    LOG.info("Creating dataflow pipeline for workflow " + workflow.getDefn().getName());
    PCollection<KV<String, WorkflowArgs>> input = p.apply(Create.of(workflowArgs));
    input = dataflow(Workflow.Steps.graph(workflow), input);

    if (workflowArgs.values().iterator().next().getDeleteFiles()) {
      LOG.info("Intermediate files will be deleted");
      input =
          input.apply(
              ParDo.named("DeleteIntermediateFiles").of(new DeleteIntermediateFiles(workflow)));
    }

    return p;
  }

  /**
   * Recursively construct the dataflow pipeline.
   *
   * @param graphItem a node, edge or branch point
   * @param input the inputs to the graph element
   * @throws IOException
   */
  private static PCollection<KV<String, WorkflowArgs>> dataflow(
      GraphItem graphItem, PCollection<KV<String, WorkflowArgs>> input) throws IOException {
    PCollection<KV<String, WorkflowArgs>> output = input;

    // It's a node
    if (graphItem instanceof Workflow) {
      Workflow w = (Workflow) graphItem;

      LOG.info("Adding task: " + w.getDefn().getName());
      output = input.apply(DockerDo.of(w));
    }
    // It's a branch
    else if (graphItem instanceof Branch) {
      LOG.info("Pipeline splits into branches. Adding branches");
      output = branches(((Branch) graphItem), input);
    }
    // It's an edge
    else if (graphItem instanceof Steps) {
      LOG.info("Adding steps");
      Steps steps = (Steps) graphItem;

      // For each sequential element, the output of one is the input to
      // the next
      if (steps.getSteps() != null) {
        for (GraphItem item : steps.getSteps()) {
          output = dataflow(item, output);
        }
      }
    } else {
      throw new IllegalStateException("Invalid graph element type: " + graphItem);
    }
    return output;
  }

  /**
   * The graph splits into parallel branches. Generate a PCollection for each, then merge them
   * together into a flattened PCollectionList, and finally combine globally so that the graph can
   * continue merged.
   *
   * <p>In order to run the branch tasks in parallel the Dataflow pipeline must have maxNumWorkers
   * == number of branches. To run Dataflow on a single VM, and support branching graphs nicely,
   * some optimization is needed.
   *
   * @param graphItem
   * @param input
   * @return the globally combined, flattened output of all edges; value is a singleton string,
   *     which is randomly chosen from the inputs
   * @throws IOException
   */
  public static PCollection<KV<String, WorkflowArgs>> branches(
      Branch graphItem, PCollection<KV<String, WorkflowArgs>> input) throws IOException {

    List<GraphItem> branches = graphItem.getBranches();
    LOG.info("Branch count: " + branches.size());

    PCollectionList<KV<String, WorkflowArgs>> outputs = null;

    // For each edge, apply a transform to the input collection
    for (GraphItem branch : branches) {
      LOG.info("Adding branch");
      PCollection<KV<String, WorkflowArgs>> branchOutput = dataflow(branch, input);
      outputs = outputs == null ? PCollectionList.of(branchOutput) : outputs.and(branchOutput);
    }

    LOG.info("Merging " + outputs.size() + " branches");
    return outputs.apply(new MergeBranches("MergeBranches" + (numMerges > 1 ? ++numMerges : "")));
  }
}
