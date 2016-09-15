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

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.runner.Operation;
import com.google.cloud.genomics.dockerflow.runner.TaskException;
import com.google.cloud.genomics.dockerflow.runner.TaskRunner;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskDefn;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a Docker task in parallel on multiple input parameter sets. Tasks run with the Pipelines API.
 *
 * <p>See the static methods for the various transforms. Most commonly, you'll want to use the
 * default {@link DockerDo#of(Task)}, which includes all of the other steps.
 *
 * @author binghamj
 */
@SuppressWarnings("serial")
public class DockerDo
    extends PTransform<
        PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DockerDo.class);

  private Task task;

  /**
   * Perform a Docker task on each input workflow parameter set, including any scatter, gather,
   * task, retries, and argument updates.
   */
  public static DockerDo of(Task t) {
    return new DockerDo(t);
  }

  /** Constructor. */
  private DockerDo(Task t) {
    super(t.getDefn().getName());

    LOG.debug("Creating DockerDo for task " + t.getDefn().getName());
    task = t;
  }

  /** Run a Docker task and block until completion. */
  public static PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>>
      run(Task t) {
    return new RunTask(t);
  }

  /** Scatter on the inputs to run multiple Docker tasks in parallel. */
  public static ParDo.Bound<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> scatter(Task t) {
    return ParDo.named("Scatter").of(new ScatterTasks(t));
  }

  /** Retry a Docker task up to {@link Task#getMaxTries()} times. */
  public static PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>>
      retry(Task t) {
    return new RetryTask(t);
  }

  /** Gather tasks into fewer workflow arg sets. */
  public static PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>>
      gather(Task t) {
    return new GatherTasks(t);
  }

  /** Retry a Docker task up to {@link Task#getMaxTries()} times. */
  public static PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>>
      outputs(Task t) {
    return new RememberOutputs(t);
  }

  /**
   * Run a task in Docker. Start concurrent tasks asynchronously, then wait for completion. Support
   * scatterBy and gatherBy, as well as tasks that update the workflow arguments for subsequent
   * tasks.
   */
  @Override
  public PCollection<KV<String, WorkflowArgs>> apply(PCollection<KV<String, WorkflowArgs>> input) {
    PCollection<KV<String, WorkflowArgs>> pc = input;

    // Scatter
    if (task.getScatterBy() != null) {
      pc = pc.apply(scatter(task));
    }

    // Run the Docker task
    pc = pc.apply(run(task));

    // Add retries
    if (task.getArgs() instanceof WorkflowArgs
        && ((WorkflowArgs) task.getArgs()).getMaxTries() > 1) {
      pc = pc.apply(retry(task));
    }

    // Pass along the log paths and other outputs
    pc = pc.apply(outputs(task));

    // Gather
    if (task.getGatherBy() != null) {
      LOG.debug("gatherBy=" + task.getGatherBy());
      pc = pc.apply(gather(task));
    }

    return pc;
  }

  static class RunTask
      extends PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {
    private Task task;
    private int attempt = 0;

    RunTask(Task task) {
      this(task, 0);
    }

    RunTask(Task task, int attempt) {
      super(attempt == 0 ? "RunTask" : "Retry-" + attempt);
      this.task = task;
      this.attempt = attempt;
    }

    @Override
    public PCollection<KV<String, WorkflowArgs>> apply(
        PCollection<KV<String, WorkflowArgs>> input) {
      PCollection<KV<String, WorkflowArgs>> pc = input;

      if (attempt == 0) {
        pc = pc.apply(ParDo.named("Prepare").of(new ClearOperationStatus()));
      }

      return pc.apply(ParDo.named("Start").of(new StartTask(task, attempt)))
          .apply(new BreakFusion<KV<String, WorkflowArgs>>("AfterStarted"))
          .apply(ParDo.named("Wait").of(new WaitForOperation(task)));
    }
  }

  static class StartTask extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {
    private static final Logger LOG = LoggerFactory.getLogger(StartTask.class);
    private Task task;
    private int attempt;

    StartTask(Task t) {
      this(t, 0);
    }

    StartTask(Task t, int attempt) {
      LOG.debug("Creating DoFn RunDocker for task " + t.getDefn().getName());
      this.task = t;
      this.attempt = attempt;
    }

    @Override
    public void processElement(
        DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c) {
      LOG.info(
          "Preparing to start task "
              + task.getDefn().getName()
              + " for key "
              + c.element().getKey());

      Operation o = c.element().getValue().getCurrentOperation();
      if (o != null) {
        LOG.info("Current operation: " + o.getName());
      }

      // Exit if the task has already completed without error
      // This happens if retries are enabled but the first try succeeded
      if (o != null && o.getDone() && o.getError() == null) {
        c.output(c.element());
      // Otherwise run (or rerun) it
      } else {
        // Modify a copy of the task
        Task t = new Task(task);

        // Get extra settings like test, abortOnError, etc
        t.setArgs(new WorkflowArgs(task.getArgs()));
        WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
        t.applyArgs(wa);

        // For failed tasks, disable preemptible VMs for the final try
        if (o != null
            && o.getError() != null
            && t.getArgs() instanceof WorkflowArgs
            && attempt >= ((WorkflowArgs) t.getArgs()).getMaxTries() - 1
            && t.getArgs().getResources() != null) {

          LOG.info("Using standard VM for final try");
          t.getArgs().getResources().setPreemptible(false);
        }

        // If resuming a failed run, check if the outputs already exist; if so, skip
        if (t.getArgs() instanceof WorkflowArgs
            && ((WorkflowArgs) t.getArgs()).getResumeFailedRun()
            && outputsExist(t)) {

          LOG.info("Skipping task. All output files exist");
          o = new Operation();
          o.setDone(true);
          o.setName("operations/RESUME-" + t.hashCode());
        // Submit the task
        } else {
          LOG.info("WorkflowArgs: " + StringUtils.toJson(wa));

          LOG.info("Starting task");
          try {
            o = TaskRunner.runTask(t);
          } catch (IOException e) {
            String msg =
                "Error starting Docker task "
                    + t.getDefn().getName()
                    + ". Cause: "
                    + e.getMessage();
            throw new TaskException(msg, e);
          }
        }
        wa.setCurrentOperation(o);

        c.output(KV.of(c.element().getKey(), wa));
      }
    }

    /** All output files from the task already exist. */
    private boolean outputsExist(Task t) {

      LOG.info("Checking if outputs exist");
      boolean exist = true;

      // Remember output file locations so future tasks can reference them
      if (t.getDefn().getOutputParameters() != null) {
        for (Param p : t.getDefn().getOutputParameters()) {
          String path = p.getDefaultValue();
          if (t.getArgs().get(p.getName()) != null) {
            path = t.getArgs().get(p.getName());
          }
          exist = exist && FileUtils.gcsPathExists(path);

          if (!exist) {
            LOG.info("Path does not exist: " + path);
            break;
          }
        }
      }

      // Remember log file locations so future tasks can reference them
      if (exist) {
        String log = t.getArgs().getLogging().getGcsPath();
        exist =
            exist
                && FileUtils.gcsPathExists(FileUtils.logPath(log, null))
                && FileUtils.gcsPathExists(FileUtils.stdoutPath(log, null))
                && FileUtils.gcsPathExists(FileUtils.stderrPath(log, null));
        if (!exist) {
          LOG.info("Log files do not: " + FileUtils.stdoutPath(log, null));
        }
      }

      LOG.info("Outputs " + (exist ? "DO" : "DO NOT") + " already exist");
      return exist;
    }
  }

  static class ScatterTasks extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {
    private static final Logger LOG = LoggerFactory.getLogger(ScatterTasks.class);
    private Task task;

    ScatterTasks(Task t) {
      task = t;
    }

    @Override
    public void processElement(
        DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c) {
      String scatterBy = task.getScatterBy();

      if (scatterBy == null) {
        LOG.info("No scatterBy field defined");
        c.output(c.element());
      } else {
        LOG.info("Scattering by " + scatterBy);

        Task t = new Task(task);
        WorkflowArgs wa = new WorkflowArgs(c.element().getValue());

        // Call with isScattering==true to disable inputBinding resolution
        t.applyArgs(wa, true);

        String key = c.element().getKey();
        String multi = null;

        // Look in the task args
        if (t.getArgs() != null && t.getArgs().contains(scatterBy)) {
          LOG.info("Scatter on task args: " + t.getArgs().get(scatterBy));
          multi = t.getArgs().get(scatterBy);
        }
        // Look in the task defn
        else {
          Param p = t.getDefn().getInput(scatterBy);
          if (p.getDefaultValue() == null) {
            LOG.info("No defaultValue to scatter by");
            multi = "";
          } else {
            LOG.info("Scatter on defaultValue: " + p.getDefaultValue());
            multi = p.getDefaultValue();
          }

          // The parameter doesn't exist!
          if (multi == null) {
            throw new IllegalStateException("Unable to find scatterBy field " + scatterBy);
          }
        }
        String[] values = multi.split(Task.REGEX_FOR_SCATTER);

        // Create a new copy of the workflow args with each shard's
        // value
        LOG.info("Creating " + values.length + " shards for key " + key);
        int digits = String.valueOf(values.length).length();
        int shardIndex = 1;
        for (String val : values) {
          LOG.info("Creating shard: " + val);
          WorkflowArgs shard = new WorkflowArgs(wa);

          // After sharding, values cannot be loaded from file
          shard.setFromFile(scatterBy, false);

          shard.set(t.getDefn().getName() + "." + scatterBy, val);

          // Make a sortable string name
          String shardName = String.format("%0" + digits + "d", shardIndex);

          if (shard.contains(Task.TASK_SHARD)) {
            shardName = shard.get(Task.TASK_SHARD) + "-" + shardName;
          }
          shard.set(Task.TASK_SHARD, shardName);

          c.output(KV.of(key, shard));
          ++shardIndex;
        }
        LOG.info("Done scattering");
      }
    }
  }

  static class GatherTasks
      extends PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {
    private static final Logger LOG = LoggerFactory.getLogger(GatherTasks.class);
    private Task task;

    GatherTasks(Task t) {
      super("Gather");
      task = t;
    }

    @Override
    public PCollection<KV<String, WorkflowArgs>> apply(
        PCollection<KV<String, WorkflowArgs>> input) {
      return input
          .apply(ParDo.named("Prepare").of(new Gather(task)))
          .apply(Combine.perKey(new SortArgs()))
          .apply(ParDo.named("CombineOutputs").of(new CombineArgs()));
    }

    /** Wrapper to override Java Collections serialization behavior for Dataflow. */
    static class Wrapper implements Serializable {
      public TreeMap<String, WorkflowArgs> map = new TreeMap<String, WorkflowArgs>();
    }

    static class Gather extends DoFn<KV<String, WorkflowArgs>, KV<String, Wrapper>> {
      private Task task;

      Gather(Task t) {
        task = t;
      }

      @Override
      public void processElement(
          DoFn<KV<String, WorkflowArgs>, KV<String, Wrapper>>.ProcessContext c) {
        String key = c.element().getKey();
        WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
        String shard = String.valueOf(-1);

        // Clear the shard index from previous scattering
        if (wa.getInputs() != null && wa.getInputs().containsKey(Task.TASK_SHARD)) {
          shard = wa.getInputs().get(Task.TASK_SHARD);
          wa.getInputs().remove(Task.TASK_SHARD);
        }

        if (task.getGatherBy() == null) {
          LOG.info("No gatherBy field defined");
        } else {
          String gatherBy = task.getGatherBy();
          LOG.info("Gathering by " + gatherBy);

          Task t = new Task(task);
          t.applyArgs(wa); // in case there are globals

          // Change the key in the KV to be the value to gather by, so
          // we can group by key
          if (t.getArgs() != null
              && t.getArgs().getInputs() != null
              && t.getArgs().getInputs().containsKey(gatherBy)) {
            key = t.getArgs().getInputs().get(gatherBy);
          } else {
            TaskDefn.Param p = t.getDefn().getInput(gatherBy);

            // The parameter doesn't exist!
            if (p == null) {
              throw new IllegalStateException("Unable to find gatherBy field " + gatherBy);
            }

            if (p.getDefaultValue() != null) {
              key = p.getDefaultValue();
            } else {
              throw new IllegalStateException(
                  "Attempt to gatherBy an undefined value of param " + gatherBy);
            }
          }
        }

        Wrapper w = new Wrapper();
        w.map.put(shard, wa);
        c.output(KV.of(key, w));
      }
    }

    /** Combine into a sorted map. */
    static class SortArgs implements SerializableFunction<Iterable<Wrapper>, Wrapper> {

      @Override
      public Wrapper apply(Iterable<Wrapper> input) {

        LOG.info("Sorting");
        Wrapper retval = new Wrapper();

        for (Wrapper w : input) {
          retval.map.putAll(w.map);
        }

        LOG.info("Elements: " + retval.map.size());
        return retval;
      }
    }

    /** Combine input / output values in order. */
    static class CombineArgs extends DoFn<KV<String, Wrapper>, KV<String, WorkflowArgs>> {

      @Override
      public void processElement(
          DoFn<KV<String, Wrapper>, KV<String, WorkflowArgs>>.ProcessContext c) throws Exception {

        LOG.info("Combining args");

        Wrapper value = c.element().getValue();
        WorkflowArgs retval = null;

        // Iterate in order
        for (WorkflowArgs wa : value.map.values()) {

          // Modify a copy
          if (retval == null) {
            retval = new WorkflowArgs(wa);
          // Find differences and merge
          } else {
            retval.gatherArgs(wa);
          }
        }
        c.output(KV.of(c.element().getKey(), retval));
      }
    }
  }

  static class RetryTask
      extends PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {
    private Task task;

    RetryTask(Task task) {
      super("Retry");
      this.task = task;
    }

    @Override
    public PCollection<KV<String, WorkflowArgs>> apply(
        PCollection<KV<String, WorkflowArgs>> input) {
      PCollection<KV<String, WorkflowArgs>> pc = input;

      // Add retries
      for (int i = 1; i < ((WorkflowArgs) task.getArgs()).getMaxTries(); ++i) {
        pc = pc.apply(new RunTask(task, i));
      }
      return pc;
    }
  }

  static class RememberOutputs
      extends PTransform<
          PCollection<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {
    private Task task;

    RememberOutputs(Task task) {
      super("Outputs");
      this.task = task;
    }

    @Override
    public PCollection<KV<String, WorkflowArgs>> apply(
        PCollection<KV<String, WorkflowArgs>> input) {
      return input.apply(ParDo.of(new Outputs(task)));
    }

    static class Outputs extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {
      private Task task;

      Outputs(Task t) {
        this.task = t;
      }

      @Override
      public void processElement(
          DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c) {
        String key = c.element().getKey();
        WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
        Task t = new Task(task);
        t.applyArgs(wa);

        Operation o = wa.getCurrentOperation();

        if (wa.getOutputs() == null) {
          wa.setOutputs(new LinkedHashMap<String, String>());
        }

        // Remember output file locations so future tasks can reference them
        if (t.getDefn().getOutputParameters() != null) {
          for (Param p : t.getDefn().getOutputParameters()) {
            wa.getOutputs().put(t.getDefn().getName() + "." + p.getName(), p.getDefaultValue());
          }
        }
        // Use the value from the args, if set, rather than the default
        if (t.getArgs().getOutputs() != null) {
          for (String name : t.getArgs().getOutputs().keySet()) {
            wa.getOutputs().put(t.getDefn().getName() + "." + name, t.getArgs().getOutputs().get(name));
          }
        }

        // Remember log file locations so future tasks can reference them
        String name = t.getDefn().getName();
        String log = t.getArgs().getLogging().getGcsPath();
        wa.getOutputs().put(name + "." + Task.TASK_LOG, FileUtils.logPath(log, o.getName()));
        wa.getOutputs().put(name + "." + Task.TASK_STDOUT, FileUtils.stdoutPath(log, o.getName()));
        wa.getOutputs().put(name + "." + Task.TASK_STDERR, FileUtils.stderrPath(log, o.getName()));

        c.output(KV.of(key, wa));
      }
    }
  }

  static class ClearOperationStatus
      extends DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>> {

    @Override
    public void processElement(
        DoFn<KV<String, WorkflowArgs>, KV<String, WorkflowArgs>>.ProcessContext c)
        throws Exception {
      WorkflowArgs wa = new WorkflowArgs(c.element().getValue());
      wa.setCurrentOperation(null);
      c.output(KV.of(c.element().getKey(), wa));
    }
  }
}
