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
import com.google.cloud.genomics.dockerflow.args.TaskArgs;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.task.Task;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static directed acyclic graph definition that can be executed on Dataflow. It's a
 * generalization of the Pipelines API RunPipelineRequest to support workflow graphs. (It's not a
 * derived class, because RunPipelineRequest is a final class.)
 *
 * <p>To pass command-line parameter overrides to subtasks, qualify with the task name like
 * "task1_name.inputName=value", "task2_name.outputName=value", etc.
 */
@SuppressWarnings("serial")
public class Workflow extends Task {
  static final Logger LOG = LoggerFactory.getLogger(Workflow.class);

  private String workflowDefnFile;
  private List<Object> graph;
  private List<Workflow> steps;
  private Steps dag;

  public Workflow() {}

  public Workflow(Task t) {
    super(t);
    if (t.getArgs() != null) {
      args = new WorkflowArgs(t.getArgs());
    }
  }

  /** Recursively set global variables. Called by {@link #applyArgs(TaskArgs)}. */
  @Override
  public void substitute(Map<String, String> globals) {
    super.substitute(globals);

    if (steps != null) {
      for (Workflow w : steps) {
        w.substitute(globals);
      }
    }
  }

  /** Recursively set parameters in the subtasks. */
  @Override
  public void applyArgs(TaskArgs a) {
    super.applyArgs(a);

    if (steps != null) {
      for (Workflow w : steps) {
        w.applyArgs(a);
      }
    }
  }

  public String getWorkflowDefnFile() {
    return workflowDefnFile;
  }

  public void setWorkflowDefnFile(String file) {
    this.workflowDefnFile = file;
  }

  public List<Workflow> getSteps() {
    return steps;
  }

  public void setSteps(List<Workflow> steps) {
    this.steps = steps;
  }

  /** Set the steps from a graph. */
  public void setSteps(Steps graph) {
    dag = graph;
    if (graph != null && graph.getSteps() != null) {
      if (steps == null) {
        steps = new ArrayList<Workflow>();
      }
      steps.addAll(steps(graph));
    }
  }

  /** Recursively descend the graph and add all tasks. */
  private List<Workflow> steps(GraphItem graphItem) {
    List<Workflow> retval = new ArrayList<Workflow>();

    if (graphItem instanceof Workflow) {
      Workflow w = (Workflow) graphItem;

      if (w.getSteps() == null || w.getSteps().isEmpty()) {
        retval.add(w);
      } else {
        retval.addAll(w.getSteps());
      }
    } else if (graphItem instanceof Branch) {
      Branch b = (Branch) graphItem;
      for (GraphItem branch : b.getBranches()) {
        retval.addAll(steps(branch));
      }
    } else if (graphItem instanceof Steps) {
      Steps g = (Steps) graphItem;
      for (GraphItem step : g.getSteps()) {
        retval.addAll(steps(step));
      }
    }
    return retval;
  }

  /** The graph as yaml/json using task names only. */
  public List<Object> getGraph() {
    return graph;
  }

  /** The graph as yaml/json using task names only. */
  public void setGraph(List<Object> graph) {
    this.graph = graph;
  }

  /** Find the step in the workflow having the desired name. */
  public Workflow step(String name) {
    for (Workflow step : steps) {
      if (name.equals(step.getDefn().getName())) {
        return step;
      }
    }
    throw new IllegalStateException("Workflow step not found: " + name);
  }

  /** The directed acyclic graph to be converted into a Dataflow pipeline. */
  public Steps getDAG() {
    if (dag == null) {
      dag = Steps.of(this);
    }
    return dag;
  }

  /** A branch in a directed acyclic graph. */
  public static class Branch implements GraphItem, Serializable {
    private List<GraphItem> branches;

    public static GraphItem of(GraphItem... graphItems) {
      Branch b = new Branch();
      b.setBranches(Arrays.asList(graphItems));
      return b;
    }

    public List<GraphItem> getBranches() {
      return branches;
    }

    public void setBranches(List<GraphItem> branches) {
      this.branches = branches;
    }
  }

  /** A directed acyclic graph. */
  public static class Steps implements GraphItem, Serializable {
    static final Logger LOG = LoggerFactory.getLogger(Steps.class);

    private List<GraphItem> steps;

    public static Steps of(GraphItem... graphItems) {
      Steps g = new Steps();
      g.steps = Arrays.asList(graphItems);
      return g;
    }

    public static Steps of(Workflow workflow) {
      return graph(workflow);
    }

    public List<GraphItem> getSteps() {
      return steps;
    }

    public void setSteps(List<GraphItem> steps) {
      this.steps = steps;
    }

    /**
     * Get the directed acyclic graph (DAG) of all workflow objects. The workflow definition file
     * contains just the task name strings to keep it human-readable. But that means we now have to
     * create an identically shaped graph that actually has the full task definition for each node
     * -- and each node could itself expand into a graph.
     *
     * @param w
     * @return a list of sequential steps in a directed acyclic graph
     */
    public static Steps graph(Workflow w) {
      LOG.info("Creating graph for workflow " + w.getDefn().getName());
      Steps graph = new Steps();
      graph.setSteps(new ArrayList<GraphItem>());

      // It's a singleton pipeline
      if (w.getDefn() != null && (w.getSteps() == null || w.getSteps().isEmpty())) {
        LOG.info("Add workflow to graph: " + w.getDefn().getName());
        graph.getSteps().add(w);
      // It's a DAG defined in code
      } else if (w.dag != null) {
        return w.getDAG();
      } else {
        // No DAG defined; run steps in order
        if (w.getGraph() == null || w.getGraph().isEmpty()) {
          graph.getSteps().addAll(w.getSteps());
        }

        // DAG is defined in yaml/json
        if (w.getGraph() != null) {
          for (Object node : w.getGraph()) {
            GraphItem subgraph = subgraph(node, w);
            graph.getSteps().add(subgraph);
          }
        }
      }
      return graph;
    }

    /**
     * Recursive function that constructs the graph nodes (workflows), branches (maps with exactly
     * one key, "BRANCH"), and edges (lists of sequential steps).
     *
     * @param graphElement an element in the graph definition
     * @param workflow
     * @return the top-most node in a directed acyclic subgraph
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static GraphItem subgraph(Object graphElement, Workflow workflow) {
      LOG.info("Get subgraph");

      GraphItem subgraph = null;

      // It's a node. Add a single linear step in sequence
      if (graphElement instanceof String) {
        LOG.info("Subgraph is a single node named: " + graphElement);

        Workflow step = workflow.step((String) graphElement);
        Steps tasks = graph(step);

        // The step might be an individual task or a whole workflow
        subgraph = tasks.getSteps().size() == 1 ? tasks.getSteps().get(0) : tasks;
      // Branch to multiple parallel steps
      } else if (isBranch(graphElement)) {
        LOG.info("Subgraph is a BRANCH");

        Branch branch = new Branch();

        List<Object> branches = (List<Object>) ((Map) graphElement).get(DockerflowConstants.BRANCH);
        LOG.info("Branch count: " + branches.size());

        List<GraphItem> newBranches = new ArrayList<GraphItem>(branches.size());
        branch.setBranches(newBranches);

        // Add a subgraph for each branch
        for (Object subnode : branches) {
          LOG.info("Adding branch");
          newBranches.add(subgraph(subnode, workflow));
        }
        subgraph = branch;
      // It's an edge. Add a subsequence of steps
      } else if (graphElement instanceof List) {
        List<GraphItem> steps = new ArrayList<GraphItem>();
        Steps g = new Steps();
        g.setSteps(steps);

        for (Object step : (List) graphElement) {
          steps.add(subgraph(step, workflow));
        }
        subgraph = g;
      } else {
        throw new IllegalStateException(
            "Malformed pipeline graph for datapipe "
                + workflow.getDefn().getName()
                + " at node "
                + graphElement);
      }
      return subgraph;
    }

    /** The graph element is a branch point -- ie, it's a map with key "BRANCH" = List<nodes>. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean isBranch(Object graphElement) {
      return graphElement instanceof Map
          && ((Map) graphElement).size() == 1
          && (((Map<String, Object>) graphElement).keySet().contains(DockerflowConstants.BRANCH));
    }
  }
}
