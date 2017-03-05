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

  public static TaskBuilder named(String name) {
    return new TaskBuilder(name);
  }

  public static TaskBuilder fromFile(String path) throws IOException, URISyntaxException {
    TaskBuilder tb = new TaskBuilder();
    tb.task = WorkflowFactory.load(path);
    return tb;
  }

  public static TaskBuilder fromTask(Task t, String name) {
    TaskBuilder tb = new TaskBuilder();
    tb.task = new Workflow(t);
    tb.task.getDefn().setName(name);
    return tb;
  }

  public static TaskBuilder fromTaskDefnFile(String path) throws IOException {
    TaskBuilder tb = new TaskBuilder(null);
    tb.task.setDefn(WorkflowFactory.loadDefn(path));
    return tb;
  }

  public TaskBuilder withArgs(TaskArgs args) {
    task.setArgs(args);
    return this;
  }

  public TaskBuilder name(String name) {
    task.getDefn().setName(name);
    return this;
  }

  public TaskBuilder docker(String imageName) {
    if (task.getDefn().getDocker() == null) {
      task.getDefn().setDocker(new Docker());
    }
    task.getDefn().getDocker().setImageName(imageName);
    return this;
  }

  public TaskBuilder input(String name) {
    return input(name, null);
  }

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

  public TaskBuilder inputFromFile(String name, String value) {
    input(name, value);
    task.getArgs().setFromFile(name, true);
    return this;
  }

  public TaskBuilder inputArray(String name, String delimiter) {
    return inputArray(name, delimiter, (String[]) null);
  }

  public TaskBuilder inputArray(String name, String delimiter, String value) {
    return inputArray(name, delimiter, new String[] {value});
  }

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

  public TaskBuilder script(String script) {
    if (task.getDefn().getDocker() == null) {
      task.getDefn().setDocker(new Docker());
      task.getDefn().getDocker().setImageName(TaskBuilder.DEFAULT_DOCKER_IMAGE);
    }
    task.getDefn().getDocker().setCmd(script);
    return this;
  }

  public TaskBuilder scriptFile(String path) throws IOException {
    return script(FileUtils.readAll(path));
  }

  public TaskBuilder cpu(Integer cores) {
    return cpu(String.valueOf(cores));
  }

  public TaskBuilder cpu(String cores) {
    task.getDefn().getResources().setMinimumCpuCores(cores);
    return this;
  }

  public TaskBuilder memory(Number gb) {
    return memory(String.valueOf(gb));
  }

  public TaskBuilder memory(String gb) {
    task.getDefn().getResources().setMinimumRamGb(gb);
    return this;
  }

  public TaskBuilder diskSize(Integer gb) {
    return diskSize(String.valueOf(gb));
  }

  public TaskBuilder diskSize(String gb) {
    task.getDefn().getResources().getDisks().get(0).setSizeGb(gb);
    return this;
  }

  public TaskBuilder preemptible(boolean b) {
    task.getDefn().getResources().setPreemptible(b);
    return this;
  }

  public TaskBuilder zones(String[] zones) {
    task.getDefn().getResources().setZones(WorkflowFactory.expandZones(zones));
    return this;
  }

  public TaskBuilder gatherBy(String inputName) {
    task.setGatherBy(inputName);
    return this;
  }

  public TaskBuilder scatterBy(String inputName) {
    task.setScatterBy(inputName);
    return this;
  }

  public TaskBuilder clientId(String id) {
    task.getArgs().setClientId(id);
    return this;
  }

  public TaskBuilder serviceAccountEmail(String email) {
    task.getArgs().getServiceAccount().setEmail(email);
    return this;
  }

  public TaskBuilder serviceAccountScopes(List<String> scopes) {
    task.getArgs().getServiceAccount().setScopes(scopes);
    return this;
  }

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

  public TaskBuilder testing(boolean b) {
    ((WorkflowArgs)task.getArgs()).setTesting(b);
    return this;
  }

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

  public TaskBuilder graph(Object... graph) {
    return graph(Arrays.asList(graph));
  }

  public TaskBuilder graph(List<Object> graph) {
    task.setGraph(graph);
    return this;
  }

  public TaskBuilder steps(Task... tasks) {
    return steps(Arrays.asList(tasks));
  }

  public TaskBuilder steps(List<Task> tasks) {
    List<Workflow> w = new ArrayList<Workflow>();
    for (Task t : tasks) {
      w.add((Workflow) t);
    }
    task.setSteps(w);
    return this;
  }

  public TaskBuilder steps(Steps steps) {
    task.setSteps(steps);
    return this;
  }

  /** Declare the expected parameters and set default values. */
  public TaskBuilder args(WorkflowArgs args) {
    task.setArgs(args);
    return this;
  }

  public TaskBuilder args(String[] args) throws IOException {
    task.setArgs(WorkflowFactory.createArgs(args));
    return this;
  }
}
