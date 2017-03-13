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
package com.google.cloud.genomics.dockerflow.task;

import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.args.ArgsBuilder;
import com.google.cloud.genomics.dockerflow.args.TaskArgs;
import com.google.cloud.genomics.dockerflow.args.TaskArgs.Logging;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Disk;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Docker;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.LocalCopy;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Resources;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Steps;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import org.yaml.snakeyaml.Yaml;

/** Task / workflow builder for more concise creation. */
public class TaskBuilder {
  public static final String DEFAULT_DOCKER_IMAGE = "ubuntu";
  public static final String[] DEFAULT_ZONES = new String[] {"us-*"};

  protected Workflow task;

  TaskBuilder(String name) {
    task = new Workflow();

    task.setArgs(ArgsBuilder.of().build());

    if (task.getDefn() == null) {
      task.setDefn(new TaskDefn());
    }
    task.getDefn().setName(name);

    Resources r = new Resources();
    task.getDefn().setResources(r);

    // Add default disk and zones
    r.setDisks(new ArrayList<Disk>());
    r.getDisks().add(TaskDefn.Disk.defaultDisk());
    r.setZones(WorkflowFactory.expandZones(TaskBuilder.DEFAULT_ZONES));
  }

  TaskBuilder() {}

  /**
   * Create a new task with a given name.
   */
  public static TaskBuilder named(String name) {
    return new TaskBuilder(name);
  }

  /**
   * Create a new task from a YAML workflow definition file on local disk or in Cloud Storage.
   */
  public static TaskBuilder fromFile(String path) throws IOException, URISyntaxException {
    TaskBuilder tb = new TaskBuilder();
    tb.task = WorkflowFactory.load(path);
    return tb;
  }

  /**
   * Create a new task by copying an existing task and giving it a new name.
   * You can then modify the new task. Useful if you have two similar tasks
   * and don't want to repeat all of the code to define parameters.
   */
  public static TaskBuilder fromTask(Task t, String name) {
    TaskBuilder tb = new TaskBuilder();
    tb.task = new Workflow(t);
    tb.task.getDefn().setName(name);
    return tb;
  }

  /**
   * Create a new task from a YAML task definition file on local disk or in Cloud Storage.
   * A task definition is what the Pipelines API expects as its payload.
   */
  public static TaskBuilder fromTaskDefnFile(String path) throws IOException {
    TaskBuilder tb = new TaskBuilder(null);
    tb.task.setDefn(WorkflowFactory.loadDefn(path));
    return tb;
  }

  /**
   * Set or replace the arguments to the task.
   */
  public TaskBuilder withArgs(TaskArgs args) {
    task.setArgs(args);
    return this;
  }

  /**
   * Set or override the task name. 
   */
  public TaskBuilder name(String name) {
    task.getDefn().setName(name);
    return this;
  }

  /*
   * Set or replace the Docker image name in Docker hub, Google Container Registry,
   * quay.io or elsewhere. 
   */
  public TaskBuilder docker(String imageName) {
    if (task.getDefn().getDocker() == null) {
      task.getDefn().setDocker(new Docker());
    }
    task.getDefn().getDocker().setImageName(imageName);
    return this;
  }

  /**
   * Declare an input parameter that will be passed to the task as an environment variable.
   */
  public TaskBuilder input(String name) {
    return input(name, null);
  }

  /**
   * Declare an input parameter that will be passed to the task as an environment variable
   * and assign its default value. The value may be itself a variable, like ${foo} or ${PreviousTask.foo}, in
   * which case it will be evaluated lazily at runtime before executing the task.
   */
  public TaskBuilder input(String name, String value) {
    if (task.getDefn().getInputParameters() == null) {
      task.getDefn().setInputParameters(new LinkedHashSet<Param>());
    }
    Param p = task.getDefn().getParam(name) == null ? new Param() : task.getDefn().getParam(name);
    p.setName(name);
    p.setDefaultValue(value);
    task.getDefn().getInputParameters().add(p);
    return this;
  }

  /**
   * Declare an input parameter that will be passed to the task as an environment variable,
   * and assign a default value that will be loaded from a file path. 
   * The value may be itself a variable, like ${foo}, in
   * which case it will be evaluated lazily at runtime before executing the task.
   */
  public TaskBuilder inputFromFile(String name, String path) {
    input(name, path);
    task.getArgs().setFromFile(name, true);
    return this;
  }

  /**
   * Declare an input parameter array that will be passed to the task as an environment variable.
   * @param name
   * @param delimiter a parameter "foo" with delimiter " -i " will be expanded in the
   * shell command, so that "${foo}" becomes "${foo_0} -i ${foo_1} -i ${foo_2}"...
   * where foo_0, foo_1 and foo_2 are elements in the array
   */
  public TaskBuilder inputArray(String name, String delimiter) {
    return inputArray(name, delimiter, (String[]) null);
  }

  /**
   * Declare an input parameter array that will be passed to the task as an environment variable
   * and assign its default value as a whitespace delimited string.
   * The value may contain variables, like ${foo} or ${PreviousTask.foo}, in
   * which case it will be evaluated lazily at runtime before executing the task.
   * @param name
   * @param delimiter a parameter "foo" with delimiter " -i " will be expanded in the
   * shell command, so that "${foo}" becomes "${foo_0} -i ${foo_1} -i ${foo_2}"...
   * where foo_0, foo_1 and foo_2 are elements in the array
   */
  public TaskBuilder inputArray(String name, String delimiter, String value) {
    return inputArray(name, delimiter, new String[] {value});
  }

  /**
   * Declare an input parameter array that will be passed to the task as an environment variable
   * and assign its default value as an array that will be converted to a whitespace delimited string.
   * The value may be itself a variable, like ${foo} or ${PreviousTask.foo}, in
   * which case it will be evaluated lazily at runtime before executing the task.
   * @param name
   * @param delimiter a parameter "foo" with delimiter " -i " will be expanded in the
   * shell command, so that "${foo}" becomes "${foo_0} -i ${foo_1} -i ${foo_2}"...
   * where foo_0, foo_1 and foo_2 are elements in the array
   */
  public TaskBuilder inputArray(String name, String delimiter, String[] values) {
    if (task.getDefn().getInputParameters() == null) {
      task.getDefn().setInputParameters(new LinkedHashSet<Param>());
    }
    Param p = new Param();
    p.setName(name);
    p.setInputBinding(new TaskDefn.InputBinding());
    p.getInputBinding().setItemSeparator(delimiter);
    p.setType(Param.TYPE_ARRAY);

    if (values != null) {
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      for (String val : values) {
        if (!isFirst) {
          sb.append(" ");
        }
        sb.append(val);
      }
      p.setDefaultValue(sb.toString());
    }

    task.getDefn().getInputParameters().add(p);
    return this;
  }

  /**
   * Declare an input parameter for a file in GCS. The file will be accessible in the Docker
   * container at the absolute local path given by ${name}.
   */
  public TaskBuilder inputFile(String name) {
    return inputPath(name, null, null, false);
  }

  /**
   * Declare and assign an input parameter for a file in GCS. The file will be accessible in
   * the Docker container at the absolute local path given by ${name}.
   */
  public TaskBuilder inputFile(String name, String defaultGcsPath) {
    return inputPath(name, defaultGcsPath, null, false);
  }

  /**
   * Declare and assign an input parameter for a file in GCS, explicitly providing a relative
   * local path. The file will be accessible in the Docker container at the absolute local path
   * given by ${name}.
   */
  public TaskBuilder inputFile(String name, String defaultGcsPath, String localPath) {
    return inputPath(name, defaultGcsPath, localPath, false);
  }

  /**
   * Declare and assign an input parameter for a folder in GCS. The folder will be accessible in
   * the Docker container at the absolute local path given by ${name}.
   */
  public TaskBuilder inputFolder(String name) {
    return inputPath(name, null, null, true);
  }

  /**
   * Declare and assign an input parameter for a folder in GCS. The folder will be accessible in
   * the Docker container at the absolute local path given by ${name}.
   */
  public TaskBuilder inputFolder(String name, String defaultGcsPath) {
    return inputPath(name, defaultGcsPath, null, true);
  }

  /**
   * Declare and assign an input parameter for a folder in GCS, explicitly providing a relative
   * local path. The folder will be accessible in the Docker container at the absolute local path
   * given by ${name}.
   */
  public TaskBuilder inputFolder(String name, String defaultGcsPath, String localPath) {
    return inputPath(name, defaultGcsPath, localPath, true);
  }
 
  /**
   * Declare and assign an input parameter for a path in GCS, explicitly providing a relative
   * local path. The file will be accessible in the Docker container at the absolute local path
   * given by ${name}. The path can be either a file or a folder.
   */
  private TaskBuilder inputPath(String name, String defaultGcsPath, String localPath, boolean isFolder) {
    if (task.getDefn().getInputParameters() == null) {
      task.getDefn().setInputParameters(new LinkedHashSet<Param>());
    }

    // Create a local parameter with the absolute path
    Param p = task.getDefn().getParam(name) == null ? new Param() : task.getDefn().getParam(name);
    p.setName(name);
    p.setLocalCopy(new LocalCopy());
    p.getLocalCopy().setDisk(DockerflowConstants.DEFAULT_DISK_NAME);
    p.setType(isFolder ? Param.TYPE_FOLDER : Param.TYPE_FILE);

    if (defaultGcsPath != null && defaultGcsPath.indexOf("${") < 0 && localPath == null) {
      localPath = FileUtils.localPath(defaultGcsPath);
    }
    p.getLocalCopy().setPath(localPath);
    p.setDefaultValue(defaultGcsPath);

    task.getDefn().getInputParameters().add(p);

    return this;
  }
  
  /**
   * Declare an array of input files. When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder inputFileArray(String name, String delimiter) {
    return inputFileArray(name, delimiter, null, null);
  }

  /**
   * Declare and assign an array of input files. When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder inputFileArray(String name, String delimiter, String value) {
    return inputFileArray(name, delimiter, new String[] {value}, null);
  }
  
  /**
   * Declare and assign an array of input files with an explicit local path. 
   * When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder inputFileArray(
      String name, String delimiter, String[] defaultGcsPaths, String localPath) {
    if (task.getDefn().getInputParameters() == null) {
      task.getDefn().setInputParameters(new LinkedHashSet<Param>());
    }

    Param p = new Param();
    p.setName(name);
    p.setLocalCopy(new LocalCopy());
    p.getLocalCopy().setDisk(DockerflowConstants.DEFAULT_DISK_NAME);
    p.getLocalCopy().setPath(localPath);

    p.setInputBinding(new TaskDefn.InputBinding());
    p.getInputBinding().setItemSeparator(delimiter);
    p.setType(Param.TYPE_ARRAY);

    if (defaultGcsPaths != null) {
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      for (String val : defaultGcsPaths) {
        if (!isFirst) {
          sb.append(" ");
        }
        sb.append(val);
      }
      p.setDefaultValue(sb.toString());
    }

    task.getDefn().getInputParameters().add(p);
    return this;
  }

  /**
   * Declare an output parameter for a file in GCS. The file will be accessible in the Docker
   * container at the absolute local path given by ${name}.
   */
  public TaskBuilder outputFile(String name) {
    return outputPath(name, null, null, false);
  }

  /**
   * Declare and assign an output parameter for a file in GCS. The file will be accessible in
   * the Docker container at the absolute local path given by ${name}.
   */
  public TaskBuilder outputFile(String name, String defaultGcsPath) {
    return outputPath(name, defaultGcsPath, null, false);
  }

  /**
   * Declare and assign an output parameter for a file in GCS, explicitly providing a relative
   * local path. The file will be accessible in the Docker container at the absolute local path
   * given by ${name}.
   */
  public TaskBuilder outputFile(String name, String defaultGcsPath, String localPath) {
	return outputPath(name, defaultGcsPath, localPath, false);
  }

  /**
   * Declare an output parameter for a folder in GCS. The folder will be accessible in the Docker
   * container at the absolute local path given by ${name}.
   */
  public TaskBuilder outputFolder(String name) {
    return outputPath(name, null, null, true);
  }

  /**
   * Declare and assign an output parameter for a folder in GCS. The folder will be accessible in
   * the Docker container at the absolute local path given by ${name}.
   */
  public TaskBuilder outputFolder(String name, String defaultGcsPath) {
    return outputPath(name, defaultGcsPath, null, true);
  }

  /**
   * Declare and assign an output parameter for a folder in GCS, explicitly providing a relative
   * local path. The folder will be accessible in the Docker container at the absolute local path
   * given by ${name}.
   */
  public TaskBuilder outputFolder(String name, String defaultGcsPath, String localPath) {
	return outputPath(name, defaultGcsPath, localPath, true);
  }
  
  /**
   * Declare and assign an output parameter for a path in GCS, explicitly providing a relative
   * local path. The file will be accessible in the Docker container at the absolute local path
   * given by ${name}. The path can be either a file or a folder.
   */
  private TaskBuilder outputPath(String name, String defaultGcsPath, String localPath, boolean isFolder) {
    if (task.getDefn().getOutputParameters() == null) {
      task.getDefn().setOutputParameters(new LinkedHashSet<Param>());
    }

    Param p = task.getDefn().getParam(name) == null ? new Param() : task.getDefn().getParam(name);
    p.setName(name);
    p.setLocalCopy(new LocalCopy());
    p.getLocalCopy().setDisk(DockerflowConstants.DEFAULT_DISK_NAME);

    if (defaultGcsPath != null && defaultGcsPath.indexOf("${") < 0 && localPath == null) {
      localPath = FileUtils.localPath(defaultGcsPath);
    }
    p.getLocalCopy().setPath(localPath);
    p.setDefaultValue(defaultGcsPath);
    p.setType(isFolder ? Param.TYPE_FOLDER : Param.TYPE_FILE);

    task.getDefn().getOutputParameters().add(p);

    return this;
  }

  /**
   * Declare an array of output files. When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder outputFileArray(String name, String delimiter) {
    return outputFileArray(name, delimiter, null, null);
  }

  /**
   * Declare and assign an array of output files. When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder outputFileArray(String name, String delimiter, String value) {
    return outputFileArray(name, delimiter, new String[] {value}, null);
  }

  /**
   * Declare and assign an array of input files with an explicit local path. 
   * When expanded on the command-line, they will be
   * delimited. Eg, if the delimiter is " -f " and the array is "gcsPath1,gcsPath2", 
   * the command line value "-f ${name}" will be expanded as "-f localPath1 -f localPath2".
   */
  public TaskBuilder outputFileArray(
      String name, String delimiter, String[] defaultGcsPaths, String localPath) {
    if (task.getDefn().getOutputParameters() == null) {
      task.getDefn().setOutputParameters(new LinkedHashSet<Param>());
    }

    Param p = new Param();
    p.setName(name);
    p.setLocalCopy(new LocalCopy());
    p.getLocalCopy().setDisk(DockerflowConstants.DEFAULT_DISK_NAME);
    p.getLocalCopy().setPath(localPath);

    p.setInputBinding(new TaskDefn.InputBinding());
    p.getInputBinding().setItemSeparator(delimiter);
    p.setType(Param.TYPE_ARRAY);

    if (defaultGcsPaths != null) {
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      for (String val : defaultGcsPaths) {
        if (!isFirst) {
          sb.append(" ");
        }
        sb.append(val);
      }
      p.setDefaultValue(sb.toString());
    }

    task.getDefn().getOutputParameters().add(p);
    return this;
  }

  /**
   * Define the bash command that will be executed in the Docker container.
   */
  public TaskBuilder script(String script) {
    if (task.getDefn().getDocker() == null) {
      task.getDefn().setDocker(new Docker());
      task.getDefn().getDocker().setImageName(TaskBuilder.DEFAULT_DOCKER_IMAGE);
    }
    task.getDefn().getDocker().setCmd(script);
    return this;
  }

  /**
   * Define the bash command that will be executed in the Docker container,
   * loading the command from a local or Cloud Storage path.
   */
  public TaskBuilder scriptFile(String path) throws IOException {
    return script(FileUtils.readAll(path));
  }

  /**
   * Set the min CPU cores for the VM that will run the script.
   */
  public TaskBuilder cpu(Integer cores) {
    return cpu(String.valueOf(cores));
  }

  /**
   * Set the min CPU cores for the VM that will run the script.
   */
  public TaskBuilder cpu(String cores) {
    task.getDefn().getResources().setMinimumCpuCores(cores);
    return this;
  }

  /**
   * Set the min RAM GB for the VM that will run the script.
   */
  public TaskBuilder memory(Number gb) {
    return memory(String.valueOf(gb));
  }

  /**
   * Set the min RAM GB for the VM that will run the script.
   */
  public TaskBuilder memory(String gb) {
    task.getDefn().getResources().setMinimumRamGb(gb);
    return this;
  }

  /**
   * Set the disk size in GB that will be mounted to the VM.
   */
  public TaskBuilder diskSize(Integer gb) {
    return diskSize(String.valueOf(gb));
  }

  /**
   * Set the disk size in GB that will be mounted to the VM.
   */
  public TaskBuilder diskSize(String gb) {
    task.getDefn().getResources().getDisks().get(0).setSizeGb(gb);
    return this;
  }

  /**
   * Use preemptible VMs. If terminated prematurely, Dockerflow will automatically retry
   * and then escalate to a standard VM.
   */
  public TaskBuilder preemptible(boolean b) {
    task.getDefn().getResources().setPreemptible(b);
    return this;
  }

  /**
   * Launch each VM in one of these zones. Zones can include wildcards, like "us-central*".
   */
  public TaskBuilder zones(String[] zones) {
    task.getDefn().getResources().setZones(WorkflowFactory.expandZones(zones));
    return this;
  }

  /**
   * After this task completes, if it was run in multiple shards, gather the
   * shards together, combining on an input parameter. Eg, if the workflow previously
   * sharded by input file, gathering globally would lead to only one shard for
   * subsequent steps in the workflow. 
   * <p>
   * Tip: to gather globally, define an input variable with a constant value, like
   * "pipeline_run". Then gather by that value. Because the value is the same in all
   * shards, the workflow will reduce down to a single task.
   */
  public TaskBuilder gatherBy(String inputName) {
    task.setGatherBy(inputName);
    return this;
  }

  /**
   * After this task begins, scatter into multiple shards, based on multiple values
   * of an input parameter. Eg, if the workflow has an input file named "infile"
   * scattering by the input file will lead to one task shard per input file.
   * The value of "infile" must be a newline-delimited string.
   */
  public TaskBuilder scatterBy(String inputName) {
    task.setScatterBy(inputName);
    return this;
  }

  /**
   * An optional user-provided ID that will be used to tag tasks.
   */
  public TaskBuilder clientId(String id) {
    task.getArgs().setClientId(id);
    return this;
  }

  /**
   * An optional service account email. VMs will run as this service account.
   */
  public TaskBuilder serviceAccountEmail(String email) {
    task.getArgs().getServiceAccount().setEmail(email);
    return this;
  }

  /**
   * An optional list service account scopes. VMs will run with these scopes.
   */
  public TaskBuilder serviceAccountScopes(List<String> scopes) {
    task.getArgs().getServiceAccount().setScopes(scopes);
    return this;
  }

  /**
   * The cloud project id to run under. All costs will be billed to this account.
   * The running user must have authenticated against this project using gcloud before running
   * Dockerflow from the command-line.
   */
  public TaskBuilder project(String id) {
    task.getDefn().setProjectId(id);
    task.getArgs().setProjectId(id);
    return this;
  }

  /**
   * The logging path. All task logs will be stored in sub-folders. The log files will be named
   * task.log, task-stdout.log, and task-stderr.log.
   */
  public TaskBuilder logging(String path) {
    task.getArgs().setLogging(new Logging());
    task.getArgs().getLogging().setGcsPath(path);
    return this;
  }

  /**
   * Run the workflow in test mode, as a dry run. No API calls will be made. The HTTP payload
   * of the API calls will be output to stdout. Often when running in test mode, you'll want
   * to set the Dataflow runner to DirectPipelineRunner using the dockerflow --runner flag
   * on the command-line.
   */
  public TaskBuilder testing(boolean b) {
    ((WorkflowArgs)task.getArgs()).setTesting(b);
    return this;
  }

  /**
   * Return the task object. Call this after fully configuring the task.
   */
  public Workflow build() {
    return task;
  }

  /**
   * Construct a graph from a yaml or json string containing the names of the subtasks. Eg, in json:
   *
   * <pre>
   * ["stepOne", "BRANCH": ["stepTwoA", "stepTwoB"], "stepThree"]
   * </pre>
   *
   * @param yamlOrJson
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public TaskBuilder graph(String yamlOrJson) throws IOException {
    List<Object> o;
    if (yamlOrJson.startsWith("[")) {
      o = StringUtils.fromJson(yamlOrJson, ArrayList.class);
    } else {
      o = new Yaml().loadAs(yamlOrJson, ArrayList.class);
    }
    return graph(o);
  }

  /**
   * When using the builder to create a workflow rather than an individual task,
   * you can explicitly define the directed acyclic graph of workflow steps.
   * Examples: taskA, taskB, taskC.
   */
  public TaskBuilder graph(Object... graph) {
    return graph(Arrays.asList(graph));
  }

  /**
   * When using the builder to create a workflow rather than an individual task,
   * you can explicitly define the directed acyclic graph of workflow steps.
   */
  public TaskBuilder graph(List<Object> graph) {
    task.setGraph(graph);
    return this;
  }

  /**
   * When using the builder to create a workflow rather than an individual task,
   * you can explicitly define the directed acyclic graph of workflow steps.
   * This method defines a linear sequence of steps.
   */
  public TaskBuilder steps(Task... tasks) {
    return steps(Arrays.asList(tasks));
  }

  /**
   * When using the builder to create a workflow rather than an individual task,
   * you can explicitly define the directed acyclic graph of workflow steps.
   * This method defines a linear sequence of steps.
   */
  public TaskBuilder steps(List<Task> tasks) {
    List<Workflow> w = new ArrayList<Workflow>();
    for (Task t : tasks) {
      w.add((Workflow) t);
    }
    task.setSteps(w);
    return this;
  }

  /**
   * When using the builder to create a workflow rather than an individual task,
   * you can explicitly define the directed acyclic graph of workflow steps.
   * Examples: "Steps.of(taskA, taskB, taskC)";
   * "Steps.of(taskA, Branch.of(taskB, taskC))"
   */
  public TaskBuilder steps(Steps steps) {
    task.setSteps(steps);
    return this;
  }

  /** Declare the expected parameters and set default values. */
  public TaskBuilder args(WorkflowArgs args) {
    task.setArgs(args);
    return this;
  }

  /**
   * Initialize the workflow based on command-line flags passed to Dockerflow.
   */
  public TaskBuilder args(String[] args) throws IOException {
    task.setArgs(WorkflowFactory.createArgs(args));
    return this;
  }
}
