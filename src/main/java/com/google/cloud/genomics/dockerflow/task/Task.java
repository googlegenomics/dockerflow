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
import com.google.cloud.genomics.dockerflow.args.TaskArgs;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.runner.TaskException;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Disk;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.LocalCopy;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Resources;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.GraphItem;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.script.ScriptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single task that can be executed via the Pipelines API. It's a generalization of the Pipelines
 * API RunPipelineRequest to support execution within a workflow graph. (It's not a derived class,
 * because RunPipelineRequest is a final class.)
 */
@SuppressWarnings("serial")
public class Task implements Serializable, GraphItem {
  private static Logger LOG = LoggerFactory.getLogger(Task.class);

  /** When scattering by an input parameter, use this character as the delimiter: newline. */
  public static final String REGEX_FOR_SCATTER = "\\n";

  /**
   * The client id of this task. Inputs, outputs, and logging paths can reference it as
   * ${workflow.clientId}.
   */
  public static final String WORKFLOW_CLIENT_ID = "workflow.clientId";

  /**
   * If there are multiple workflow instances with different parameters, this is the index. Inputs,
   * outputs, and logging paths can reference it as ${workflow.index}.
   */
  public static final String WORKFLOW_INDEX = "workflow.index";

  /**
   * A unique identifier for the task and shard that's being run. The value is
   * [${workflow.clientId}/]{$task.name}[/${task.shard}]. Task inputs, outputs, and logging paths
   * can reference it as ${workflow.element}.
   */
  public static final String WORKFLOW_ELEMENT = "workflow.element";

  /** The name of this task. Inputs, outputs, and logging paths can reference it as ${task.name}. */
  public static final String TASK_NAME = "task.name";

  /**
   * The shard of this task. Inputs, outputs, and logging paths can reference it as ${task.shard}.
   */
  public static final String TASK_SHARD = "task.shard";

  /** The log file location for this task. It can be referenced as ${task.name}/task.log */
  public static final String TASK_LOG = "task.log";

  /** The stdout file location for this task. It can be referenced as ${task.name}/task.stdout */
  public static final String TASK_STDOUT = "task.stdout";

  /** The stderr file location for this task. It can be referenced as ${task.name}/task.stderr */
  public static final String TASK_STDERR = "task.stderr";

  /**
   * Parameter name to allow setting of the disk size for a particular task from the overall
   * workflow.
   */
  public static final String TASK_DISK_SIZE = "task.diskSize";

  protected String version;

  // From RunPipelineRequest:
  protected TaskDefn defn;
  protected TaskArgs args;

  // The Pipelines API pipeline definition file
  protected String defnFile;

  // To support task sharding:
  protected String scatterBy;
  protected String gatherBy;

  /** Constructor. */
  public Task() {
    // do nothing
  }

  /** Copy constructor. Deep copy for args; shallow for resources, docker. */
  public Task(Task t) {
    version = t.version;
    if (t.defn != null) {
      defn = new TaskDefn(t.defn);
    }
    if (t.args != null) {
      args = new TaskArgs(t.args);
    }
    defnFile = t.defnFile;
    scatterBy = t.scatterBy;
    gatherBy = t.gatherBy;
  }

  /**
   * Merge args (from the command-line) into this task. If there are inputs or outputs with prefixes
   * like "taskName.inputName", strip off the prefix and set the name and value. If they're NOT
   * prefixed, they won't be set by calling this method. You'll have to add them directly to the
   * task's args.
   *
   * @param a override with these arguments
   */
  public void applyArgs(TaskArgs a) {
    applyArgs(a, false);
  }

  /**
   * Called when scattering to skip some of the path resolution.
   *
   * @param a
   * @param isScattering
   */
  public void applyArgs(TaskArgs a, boolean isScattering) {
    if (args == null) {
      args = new TaskArgs();
    }

    // Set project, name
    if (defn != null) {
      if (defn.getName() == null && defnFile != null) {
        defn.setName(defnFile);
      }
      if (a.getProjectId() != null) {
        defn.setProjectId(a.getProjectId());
      }
    }

    // External parameters come prefixed, like taskName.paramName.
    String prefix = defn.getName() + ".";
    LOG.debug("prefix=" + prefix);

    // Update the task args
    args.applyArgs(a);
    if (defn == null
        || defn.getResources() == null
        || !Boolean.TRUE.equals(defn.getResources().getPreemptible())) {
      if (args.getResources() != null) {
        args.getResources().setPreemptible(false);
      }
    }

    // For each arg, apply it to the local args and parameter declarations
    for (String k : a.keys()) {
      LOG.debug("Parameter key=" + k);
      if (!k.startsWith(prefix)) {
        continue;
      }

      LOG.debug("Prefix matches!");
      String name = k.substring(prefix.length()); // remove prefix
      String val = a.get(k);

      // Make sure it's defined in the args or the params or else is a system var
      if (!args.contains(name)
          && !defn.hasParam(name)
          && !Task.TASK_LOG.equalsIgnoreCase(name)
          && !Task.TASK_STDOUT.equalsIgnoreCase(name)
          && !Task.TASK_STDERR.equalsIgnoreCase(name)
          && !Task.TASK_DISK_SIZE.equalsIgnoreCase(name)) {
        throw new IllegalStateException(
            "Parameter "
                + name
                + " in task "
                + (defn == null ? "null" : defn.getName())
                + " does not exist. Fully qualified name: "
                + k);
      }

      // Set disk size
      if (Task.TASK_DISK_SIZE.equalsIgnoreCase(name)) {
        if (defn.getResources() == null) {
          defn.setResources(new Resources());
        }
        if (defn.getResources().getDisks() == null) {
          defn.getResources().setDisks(new ArrayList<Disk>());
        }
        if (defn.getResources().getDisks().isEmpty()) {
          defn.getResources().getDisks().add(Disk.defaultDisk());
        }
        for (Disk d : defn.getResources().getDisks()) {
          d.setSizeGb(val);
        }
      // Include it in the args even if it wasn't before
      } else {
        if (defn.getInput(name) != null) {
          if (args.getInputs() == null) {
            args.setInputs(new LinkedHashMap<String, String>());
          }
          args.getInputs().put(name, val);
        } else if (defn.getOutput(name) != null) {
          if (args.getOutputs() == null) {
            args.setOutputs(new LinkedHashMap<String, String>());
          }
          args.getOutputs().put(name, val);
        }          
        if (a.isFromFile(k)) {
          args.setFromFile(name, true);
        }
      }
    }

    // Substitute local variable cross-references and special globals
    args.substitute(args.getInputs());
    defn.substitute(args.getInputs());
    substitute(getGlobals(a));

    // Load arg values that are being applied
    loadValuesFromFile(a, prefix);

    // Load values for this task that come from file
    loadValuesFromFile(args, null);

    // Evaluate javascript
    try {
      evalJs();
    } catch (ScriptException e) {
      throw new TaskException("Error evaluating javascript", e);
    }

    // Make all paths absolute
    resolvePaths(a);

    if (!isScattering) {
      // Auto-generate the path on local disk
      resolveLocalCopies();

      // For CWL stuff
      applyInputBindings();
      
      resolveFolders();
    }
  }

  /**
   * Load parameter values from file.
   *
   * @param a
   * @param prefix prefix like "taskName." in "taskName.paramName"
   */
  private void loadValuesFromFile(TaskArgs a, String prefix) {

    // For all args
    for (String key : a.keys()) {

      // Only load values for the current task
      if (prefix == null || key.startsWith(prefix)) {

        // If the value should be loaded from file
        if (a.isFromFile(key) && !a.get(key).startsWith("${") && !a.get(key).contains("\n")) {

          LOG.info("Load value of " + key + " from file: " + a.get(key));
          try {
            String contents = FileUtils.readAll(a.get(key));
            a.set(key, contents);
            LOG.debug("Value: " + contents);

          } catch (IOException e) {
            LOG.warn(
                "Failed to load input named \""
                    + key
                    + "\" from file: "
                    + a.get(key)
                    + ". This is expected if scattering by this value.");
          }
        }
      }
    }
  }

  private Map<String, String> getGlobals(TaskArgs a) {
    Map<String, String> globals = new LinkedHashMap<String, String>();
    String shard = ""; // for scattered tasks

    // Make input params available for substitution in other inputs,
    // outputs and logging.
    if (a.getInputs() != null) {
      // If the task has been scattered, make the name available as
      // ${task.shard}
      if (a.contains(TASK_SHARD)) {
        shard = a.get(TASK_SHARD);
      }

      // Args can be referenced as ${FULL.NAME}.
      globals.putAll(a.getInputs());
    }

    // Output files and logs can be referenced as ${task-name.task.stdout}
    if (a.getOutputs() != null) {
      globals.putAll(a.getOutputs());
    }

    // Also make clientId, task name, and logging path available as ${...}
    globals.put(WORKFLOW_CLIENT_ID, a.getClientId() == null ? "" : a.getClientId());
    if (a instanceof WorkflowArgs) {
      globals.put(WORKFLOW_INDEX, String.valueOf(((WorkflowArgs) a).getRunIndex()));
    } else {
      globals.put(WORKFLOW_INDEX, "");
    }
    globals.put(TASK_NAME, defn == null || defn.getName() == null ? "" : defn.getName());
    globals.put(TASK_SHARD, shard);

    // Build up a unique string for this workflow element in the PCollection
    // Format: [${workflow.index}/]${task.name}[${task.shard}].
    // If there's only one instance, the workflow index is omitted.
    // If only one shard, that's omitted too.
    StringBuilder sb = new StringBuilder();
    sb.append(a.getClientId() == null ? "" : a.getClientId());
    if (defn != null && defn.getName() != null) {
      if (a.getClientId() != null) {
        sb.append("/");
      }
      sb.append(defn.getName());
    }
    if (a.contains(TASK_SHARD)) {
      if (sb.length() > 0) {
        sb.append("/");
      }
      sb.append(a.get(TASK_SHARD));
    }
    globals.put(WORKFLOW_ELEMENT, sb.toString());

    return globals;
  }

  private void resolveLocalCopies() {
    LOG.debug("Updating local paths");

    // Loop through a copy to avoid concurrent modification
    for (Param p : new ArrayList<Param>(defn.getParams())) {
      String val = args.get(p.getName()) == null ? p.getDefaultValue() : args.get(p.getName());

      if (val != null && (p.isFile() || p.isFolder())) {

        if (p.getLocalCopy() == null) {
          p.setLocalCopy(new LocalCopy());
          p.getLocalCopy().setDisk(DockerflowConstants.DEFAULT_DISK_NAME);
        }

        if (p.isArray()) {
          expandArray(p);

        } else if (p.getLocalCopy().getPath() == null) {
          LOG.debug("File: " + p.getName() + ", path: " + val);
          p.getLocalCopy().setPath(FileUtils.localPath(val));

        } else {
          LOG.debug(
              "File " + p.getName() + " set to " + 
              p.getLocalCopy().getPath() + " by workflow author"); 
        }
      }
    }
  }

  /**
   * For parameters that are folders, convert to environment variables 
   * with locally resolved paths and edit the Docker command to do a
   * recursive copy of inputs and outputs.
   */
  private void resolveFolders() {
    LOG.debug("Resolving folders for recursive copy");
    
    // Distinguish between inputs and outputs
    Set<String> inputs = new HashSet<String>();
    if (defn.getInputParameters() != null) {
      for (Param p : defn.getInputParameters()) {
        inputs.add(p.getName());
      }
    }
    
    Map<String,String> gcsPaths = new LinkedHashMap<String,String>();
    Map<String,String> localPaths = new LinkedHashMap<String,String>();
    
    // Loop through a copy to avoid concurrent modification
    for (Param p : new ArrayList<Param>(defn.getParams())) {
      String val = args.get(p.getName()) == null ? p.getDefaultValue() : args.get(p.getName());

      // Folder copying is done by editing the Docker command, 
      // not by automated file staging
      if (val != null && p.isFolder()) {
        gcsPaths.put(p.getName(), val);
        localPaths.put(p.getName(), p.getLocalCopy().getPath());
        
        // Replace var in user script with local path
        defn.getDocker().setCmd(
            defn.getDocker().getCmd().replace(
                "${" + p.getName() + "}", 
                DockerflowConstants.DEFAULT_MOUNT_POINT + 
                    "/" +
                    p.getLocalCopy().getPath()));

        // Turn it into an env var
        p.setLocalCopy(null);
        p.setType(null);
                
        // Create input environment variables for each output folder
        // Remove the folder option, since it's used only by Dockerflow,
        // and is not recognized by Pipelines API
        if (!inputs.contains(p.getName())) {
          defn.getOutputParameters().remove(p);
          if (defn.getInputParameters() == null) {
            defn.setInputParameters(new LinkedHashSet<Param>());
          }
          defn.getInputParameters().add(p);
          
          // Move output folder args to input args
          if (args.getOutputs() != null && args.getOutputs().containsKey(p.getName())) {
            if (args.getInputs() == null) {
              args.setInputs(new LinkedHashMap<String,String>());
            }
            args.getInputs().put(p.getName(), args.getOutputs().get(p.getName()));
            args.getOutputs().remove(p.getName());
          }
        }
      }
    }
    
    // Pipelines API rejects an empty parameter set
    if (defn.getOutputParameters() != null && defn.getOutputParameters().isEmpty()) {
      defn.setOutputParameters(null);
    }

    final String INSTALL_GSUTIL = 
        "\n# Install gsutil\n" +
        "if ! type gsutil; then\n" +
        "  apt-get update\n" +
        "  apt-get --yes install apt-utils gcc python-dev python-setuptools ca-certificates\n" +
        "  easy_install -U pip\n" +
        "  pip install -U crcmod\n" +
        "\n" +
        "  apt-get --yes install lsb-release\n" +
        "  export CLOUD_SDK_REPO=\"cloud-sdk-$(lsb_release -c -s)\"\n" +
        "  echo \"deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main\" >> /etc/apt/sources.list.d/google-cloud-sdk.list\n" +
        "  apt-get update && apt-get --yes install curl\n" +
        "  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -\n" +
        "  apt-get update && apt-get --yes install google-cloud-sdk\n" +
        "fi\n";

    // Prepare commands to copy the folders
    StringBuilder copyInputs = new StringBuilder();
    StringBuilder copyOutputs = new StringBuilder();
    
    for (String name : gcsPaths.keySet()) {
      // Copy input folder
      if (inputs.contains(name)) {
        copyInputs.append(
            String.format(
                "mkdir -p %s/%s\n",
                DockerflowConstants.DEFAULT_MOUNT_POINT,
                localPaths.get(name)) +
            String.format(
                "\nfor ((i = 0; i < 3; i++)); do\n" +
                    "  if gsutil -m rsync -r %s/ %s/%s; then\n" +
                    "    break\n" +
                    "  elif ((i == 2)); then\n" +
                    "    2>&1 echo \"Recursive localization failed.\"\n" +
                    "    exit 1\n" +
                    "  fi\n" +
                    "done\n",
                gcsPaths.get(name),
                DockerflowConstants.DEFAULT_MOUNT_POINT,
                localPaths.get(name)));
      // Copy output folder
      } else {
        copyOutputs.append(
            String.format(
                String.format(
                        "mkdir -p %s/%s\n",
                        DockerflowConstants.DEFAULT_MOUNT_POINT,
                        localPaths.get(name)) +
                "for ((i = 0; i < 3; i++)); do\n" +
                    "  if gsutil -m rsync -r %s/%s %s/; then\n" +
                    "    break\n" +
                    "  elif ((i == 2)); then\n" +
                    "    2>&1 echo \"Recursive delocalization failed.\"\n" +
                    "    exit 1\n" +
                    "  fi\n" +
                    "done\n",
                DockerflowConstants.DEFAULT_MOUNT_POINT,
                localPaths.get(name),
                gcsPaths.get(name)));
      }
    }
    
    // Edit the Docker command to copy the folders
    if (!gcsPaths.isEmpty()) {
      defn.getDocker().setCmd(
          INSTALL_GSUTIL +
          "\n# Copy inputs\n" +
          copyInputs.toString() + 
          "\n# Run user script\n" +
          defn.getDocker().getCmd() + 
          "\n\n# Copy outputs\n" +
          copyOutputs.toString());
    }
  }

  /**
   * Resolve the GCS path in case it's relative to the workflow's output path. Do not attempt to
   * resolve against relative paths.
   *
   * @throws URISyntaxException
   */
  private void resolvePaths(TaskArgs a) {
    LOG.debug("Resolving relative paths " + a);
    if (!(a instanceof WorkflowArgs) || ((WorkflowArgs) a).getWorkspace() == null) {
      LOG.warn("TaskArgs has no base path defined. All parameter paths must be absolute");
      return;
    }

    String parentPath = ((WorkflowArgs) a).getWorkspace();

    // Substitute variables like ${workflow.element}
    parentPath = StringUtils.replaceAll(getGlobals(a), parentPath);

    // Make local testing paths absolute
    if (!parentPath.startsWith("/") && !parentPath.startsWith("gs:/")) {
      parentPath = new File(parentPath).getAbsolutePath() + "/";
    }

    LOG.info("Resolving paths vs " + parentPath);
    for (Param p : defn.getParams()) {
      LOG.debug(p.getName() + " is a file? " + p.isFile() + " is folder? " + p.isFolder());
      if (!p.isFile() && !p.isFolder()) {
        continue;
      }

      String path = args.contains(p.getName()) ? args.get(p.getName()) : p.getDefaultValue();
      if (path == null) {
        throw new NullPointerException(
            "Null value for path " + p.getName()
                + " in task " + defn.getName());
      }

      // Skip javascript that's unevaluated (like in a scatter step)
      if (StringUtils.isJavaScript(path)) {
        LOG.debug("Skipping path resolution for js: " + path);
        continue;
      }

      LOG.debug(StringUtils.toJson(p));

      // Arrays are more complicated
      String separator;
      String[] allVals;
      if (p.isArray()) {
        allVals = p.split(path);
        separator = " ";
        LOG.debug("It's an array. Values: " + allVals.length);
      // Scatter-by fields are newline delimited
      } else {
        allVals = path.split(REGEX_FOR_SCATTER);
        separator = "\n";
      }

      StringBuilder sb = new StringBuilder();

      // Resolve all paths
      for (int i = 0; i < allVals.length; ++i) {
        String s = allVals[i].trim();
        if (i > 0) {
          sb.append(separator);
        }
        try {
          sb.append(FileUtils.resolve(s, parentPath));
        } catch(Exception e) {
          String msg = "Failed to resolve path: " + s;
          if (System.getenv(DockerflowConstants.DOCKERFLOW_TEST) != null
              && Boolean.parseBoolean(System.getenv(DockerflowConstants.DOCKERFLOW_TEST))) {
            LOG.warn(msg + ". Cause: " + e.getMessage());
          } else {
            throw new IllegalStateException(msg, e);
          }
        }
      }
      LOG.debug("Resolved path to: " + sb);

      // Update to the fully resolved path
      if (args.contains(p.getName())) {
        args.set(p.getName(), sb.toString());
      } else {
        p.setDefaultValue(sb.toString());
      }
    }
  }

  private void expandArray(Param p) {
    assert (p.isArray());

    LOG.info("Expand array: " + p.getName());

    String val = args.get(p.getName()) == null ? p.getDefaultValue() : args.get(p.getName());
    String[] paths = p.split(val);

    // Array item separator
    String sep = " ";
    if (p.getInputBinding() != null && p.getInputBinding().getItemSeparator() != null) {
      sep = p.getInputBinding().getItemSeparator();
    }

    // In the script, the ENV var reference "${file}"
    // will be replaced with "${file_1} ${file_2} ..."
    // with delimiters
    StringBuilder sb = new StringBuilder();

    // Create new file params for each
    for (int i = 0; i < paths.length; ++i) {
      Param next = new Param(p);
      next.setInputBinding(null);
      next.setType(null);
      next.setName(p.getName() + "_" + (i + 1));
      next.setDefaultValue(paths[i]);
      next.getLocalCopy().setPath(FileUtils.localPath(paths[i]));

      // Build up the ENV var substitution
      if (i > 0) {
        sb.append(sep);
      }
      sb.append("${" + next.getName() + "}");

      defn.getInputParameters().add(next);
    }

    LOG.debug("Replace with: " + sb.toString());

    String cmd = defn.getDocker().getCmd();
    String var = "${" + p.getName() + "}";

    if (cmd.contains(var)) {
      cmd =
          cmd.substring(0, cmd.indexOf(var))
              + sb.toString()
              + cmd.substring(cmd.indexOf(var) + var.length());
      defn.getDocker().setCmd(cmd);
    }
    LOG.debug("Replaced: " + cmd);

    // The array values are now coded in the task defn; remove them
    // from the args so the Pipelines API doesn't choke
    if (args.getInputs() != null) {
      args.getInputs().remove(p.getName());
    }
    defn.getInputParameters().remove(p);
  }

  /**
   * Apply CWL parameter bindings, like converting arrays to delimited strings, and adding prefixes.
   */
  public void applyInputBindings() {

    // Apply bindings to the input args
    if (args != null && defn != null && args.getInputs() != null) {

      for (String name : args.getInputs().keySet()) {
        String val = args.getInputs().get(name);

        Param p = defn.getInput(name);
        if (p != null) {
          val = p.bindValue(val);
        }
        args.getInputs().put(name, val);
      }

      // Apply bindings to the default values
      if (defn.getInputParameters() != null) {
        for (Param p : defn.getInputParameters()) {
          if (p.getInputBinding() != null) {
            p.setDefaultValue(p.bindValue(p.getDefaultValue()));
          }
        }
      }
    }

    // Update the command-line with positional parameters.
    if (defn != null && defn.getInputParameters() != null && defn.getDocker() != null) {
      StringBuilder sb = new StringBuilder(defn.getDocker().getCmd());

      // Sort by position
      Set<Param> s = new TreeSet<Param>(new TaskDefn.ParamComparator());
      s.addAll(defn.getInputParameters());

      // For each parameter
      for (Param p : s) {

        // If it's positional
        if (p.getInputBinding() != null && p.getInputBinding().getPosition() != null) {
          String val = p.getDefaultValue();

          // Use the arg, not the default
          if (args != null && args.getInputs().containsKey(p.getName())) {
            val = args.getInputs().get(p.getName());
          }
          sb.append(" " + val);
        }
      }
      defn.getDocker().setCmd(sb.toString());
    }
  }

  /**
   * Evaluate javascript expressions for the values of input/output args and
   * inputParameter/outputParameter defaultValues. JavaScript is evaluated after parameter
   * substitution, so a valid expression can include variables, such as: "${= ${a} / ${b} }".
   *
   * @throws ScriptException
   */
  public void evalJs() throws ScriptException {
    if (args != null) {

      if (args.getInputs() != null) {
        for (String key : args.getInputs().keySet()) {
          String val = args.getInputs().get(key);

          if (StringUtils.isJavaScript(val)) {
            LOG.debug("Input " + key + " is js: " + val);
            val = String.valueOf(StringUtils.evalJavaScript(val));
            args.getInputs().put(key, val);
          }
        }
      }

      if (args.getOutputs() != null) {
        for (String key : args.getOutputs().keySet()) {
          String val = args.getOutputs().get(key);

          if (StringUtils.isJavaScript(val)) {
            LOG.debug("Output " + key + " is js: " + val);
            val = String.valueOf(StringUtils.evalJavaScript(val));
            args.getOutputs().put(key, val);
          }
        }
      }

      // Substitute in resource sizes
      if (args.getResources() != null) {
        args.getResources().evalJavaScript();
      }
    }

    if (defn != null) {
      if (defn.getInputParameters() != null) {

        for (Param p : defn.getInputParameters()) {
          String val = p.getDefaultValue();
          if (val != null && StringUtils.isJavaScript(val)) {
            LOG.debug("Input param " + p.getName() + " is js: " + val);
            val = String.valueOf(StringUtils.evalJavaScript(val));
            p.setDefaultValue(val);
          }
        }
      }

      if (defn.getOutputParameters() != null) {
        for (Param p : defn.getOutputParameters()) {
          String val = p.getDefaultValue();
          if (val != null && StringUtils.isJavaScript(val)) {
            LOG.debug("Output param " + p.getName() + " is js: " + val);
            val = String.valueOf(StringUtils.evalJavaScript(val));
            p.setDefaultValue(val);
          }
        }
      }

      if (defn.getResources() != null) {
        defn.getResources().evalJavaScript();
      }
    }
  }

  /**
   * Substitute variables of the form ${KEY} in all inputs, outputs, and logging paths.
   *
   * @param variables KEY/VALUE pairs
   */
  public void substitute(Map<String, String> variables) {
    if (args != null) {
      args.substitute(variables);
    }
    if (defn != null) {
      defn.substitute(variables);
    }
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public TaskDefn getDefn() {
    return defn;
  }

  public void setDefn(TaskDefn defn) {
    this.defn = defn;
  }

  public TaskArgs getArgs() {
    return args;
  }

  public void setArgs(TaskArgs args) {
    this.args = args;
  }

  public String getDefnFile() {
    return defnFile;
  }

  public void setDefnFile(String file) {
    this.defnFile = file;
  }

  /**
   * Scatter by this input parameter name. The values will be split by {@link #REGEX_FOR_SCATTER}.
   */
  public String getScatterBy() {
    return scatterBy;
  }

  /**
   * Scatter by this input parameter name. The values will be split by {@link #REGEX_FOR_SCATTER}.
   */
  public void setScatterBy(String scatterBy) {
    this.scatterBy = scatterBy;
  }

  /**
   * Gather by this input parameter name. All tasks having the same value of this parameter will be
   * grouped together, and only one instance will be run.
   */
  public String getGatherBy() {
    return gatherBy;
  }

  /**
   * Gather by this input parameter name. All tasks having the same value of this parameter will be
   * grouped together, and only one instance will be run.
   */
  public void setGatherBy(String gatherBy) {
    this.gatherBy = gatherBy;
  }
}
