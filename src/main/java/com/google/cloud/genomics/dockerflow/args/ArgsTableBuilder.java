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

import com.google.cloud.genomics.dockerflow.DockerflowConstants;
import com.google.cloud.genomics.dockerflow.args.TaskArgs.Logging;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Resources;
import com.google.cloud.genomics.dockerflow.util.FileUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for a table of workflow args to allow bulk processing.
 *
 * @author binghamj
 */
public class ArgsTableBuilder implements DockerflowConstants {
  private static final Logger LOG = LoggerFactory.getLogger(ArgsTableBuilder.class);

  private Map<String, WorkflowArgs> table;

  public static ArgsTableBuilder named(String name) {
    ArgsTableBuilder b = new ArgsTableBuilder();
    b.table = new HashMap<String, WorkflowArgs>();
    b.table.put(name, new WorkflowArgs());
    return b;
  }

  public static ArgsTableBuilder of(Workflow w) throws IOException {
    return of((WorkflowArgs) w.getArgs());
  }

  public static ArgsTableBuilder of(WorkflowArgs args) {
    ArgsTableBuilder b = new ArgsTableBuilder();
    b.table = new HashMap<String, WorkflowArgs>();
    b.table.put("args", new WorkflowArgs(args));
    return b;
  }

  public static ArgsTableBuilder of(Map<String, WorkflowArgs> args) {
    ArgsTableBuilder b = new ArgsTableBuilder();
    b.table = args;
    return b;
  }

  public static ArgsTableBuilder fromArgs(String[] args) throws IOException {
    Map<String, String> m = StringUtils.parseArgs(args);
    ArgsTableBuilder b;
    WorkflowArgs wa = ArgsBuilder.fromArgs(args).build();

    if (m.containsKey(ARGS_FILE)) {
      Map<String, String> globals = null;
      if (m.containsKey(GLOBALS)) {
        globals = StringUtils.parseParameters(m.get(GLOBALS), false);
      }
      b = ArgsTableBuilder.fromFile(m.get(ARGS_FILE)).globals(globals);
      b.parameters(wa); // apply command-line settings
    } else {
      b = ArgsTableBuilder.of(wa);
    }
    return b;
  }

  /**
   * Load from file.
   *
   * @param file yaml or json for a single set of args, or csv for multiple.
   * @return
   * @throws IOException
   */
  public static ArgsTableBuilder fromFile(String file) throws IOException {
    if (file == null) {
      throw new IllegalArgumentException("File cannot be null");
    }
    ArgsTableBuilder b = new ArgsTableBuilder();

    if (file.toLowerCase().endsWith(".csv")) {
      b.table = loadCsv(file);
    }
    // Parse from yaml/json
    else {
      WorkflowArgs wa = FileUtils.parseFile(file, WorkflowArgs.class);
      b.table = new HashMap<String, WorkflowArgs>();
      b.table.put(file, wa);
    }
    return b;
  }

  ArgsTableBuilder() {}

  /**
   * Load the workflow arguments from a CSV file. The header of the CSV contains the input or output
   * parameter names. Each row contains the workflow args for a single run. To run 100 instances of
   * a workflow concurrently, create a CSV with a header row plus 100 rows for each set of
   * parameters.
   *
   * <p>Columns by default are input parameters, passed as environment variables to the Docker
   * script. For file parameters, you can prefix the column header with "<" for input or ">" for
   * output. For clarity, you can also prefix the regular input parameters as "<", if you like.
   *
   * <p>The column header can also be "logging", which is a reserved name for the logging path.
   *
   * @param csvFile CSV file (RFC4180) that's local or in GCS
   * @return a map with the key being the clientId
   * @throws IOException
   */
  static Map<String, WorkflowArgs> loadCsv(String csvFile) throws IOException {
    Map<String, WorkflowArgs> retval = new HashMap<String, WorkflowArgs>();

    String csv = FileUtils.readAll(csvFile);
    CSVParser parser = CSVParser.parse(csv, CSVFormat.RFC4180);

    // Parse header
    List<String> header = null;

    int row = 0;

    // Parse by row
    for (CSVRecord csvRecord : parser) {
      ArgsBuilder args = ArgsBuilder.of(String.valueOf(row));

      LOG.debug(StringUtils.toJson(csvRecord));

      // Parse header the first time
      if (row == 0) {
        header = new ArrayList<String>();
        for (String col : csvRecord) {
          header.add(col);
        }
      } else {
        // Set parameter defined in each column
        for (int col = 0; col < header.size(); ++col) {
          String name = header.get(col);
          String val = csvRecord.get(col);

          if (name.startsWith(PREFIX_INPUT)) {
            name = name.replace(PREFIX_INPUT, "");
            args.input(name, val);
          } else if (name.startsWith(PREFIX_OUTPUT)) {
            name = name.replace(PREFIX_OUTPUT, "");
            args.output(name, val);
          } else if (LOGGING.equals(name)) {
            args.logging(val);
          } else {
            args.input(name, val);
          }
        }
        WorkflowArgs a = args.build();
        a.setRunIndex(row);
        retval.put(a.getClientId(), a);
      }
      ++row;
    }
    return retval;
  }

  /**
   * Substitute global variables of the form ${KEY} in all inputs, outputs, and logging paths where
   * they occur.
   *
   * @param args
   * @param globals
   */
  public ArgsTableBuilder globals(Map<String, String> globals) {
    for (WorkflowArgs a : table.values()) {
      a.substitute(globals);
    }
    return this;
  }

  /**
   * Substitute a global variable of the form ${KEY} in all inputs, outputs, and logging paths where
   * it occurs.
   *
   * @param args
   * @param globals
   */
  public ArgsTableBuilder global(String key, String value) {
    Map<String, String> globals = new HashMap<String, String>();
    globals.put(key, value);

    for (WorkflowArgs a : table.values()) {
      a.substitute(globals);
    }
    return this;
  }

  /**
   * Set project, logging, resources on all workflow args.
   *
   * @param args
   * @param defaults
   */
  public ArgsTableBuilder parameters(TaskArgs values) {
    for (WorkflowArgs a : table.values()) {
      a.applyArgs(values);
    }
    return this;
  }

  public ArgsTableBuilder project(String name) {
    TaskArgs args = new TaskArgs();
    args.setProjectId(name);
    parameters(args);
    return this;
  }

  public ArgsTableBuilder logging(String path) {
    TaskArgs args = new TaskArgs();
    args.setLogging(new Logging());
    args.getLogging().setGcsPath(path);
    parameters(args);
    return this;
  }

  public ArgsTableBuilder preemptible(boolean b) {
    TaskArgs args = new TaskArgs();
    args.setResources(new Resources());
    args.getResources().setPreemptible(b);
    parameters(args);
    return this;
  }

  public ArgsTableBuilder cores(int minCpuCores) {
    TaskArgs args = new TaskArgs();
    args.setResources(new Resources());
    args.getResources().setMinimumCpuCores(String.valueOf(minCpuCores));
    parameters(args);
    return this;
  }

  public ArgsTableBuilder memory(double minRamGb) {
    TaskArgs args = new TaskArgs();
    args.setResources(new Resources());
    args.getResources().setMinimumRamGb(String.valueOf(minRamGb));
    parameters(args);
    return this;
  }

  public ArgsTableBuilder testing(boolean b) {
    WorkflowArgs args = new WorkflowArgs();
    args.setTesting(b);
    parameters(args);
    return this;
  }

  public Map<String, WorkflowArgs> build() {
    return table;
  }
}
