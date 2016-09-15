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

import com.google.cloud.genomics.dockerflow.TestUtils;
import com.google.cloud.genomics.dockerflow.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test running with multiple input parameter sets.
 */
public class ArgsTableBuilderTest {
  private static final Logger LOG = LoggerFactory.getLogger(ArgsTableBuilderTest.class);

  @Test
  public void testLoadCsv() throws IOException {
    Map<String, WorkflowArgs> m =
        ArgsTableBuilder.fromFile(TestUtils.RESOURCE_DIR + "/workflowArgs.csv")
            .project(TestUtils.TEST_PROJECT)
            .preemptible(true)
            .build();

    String json = StringUtils.toJson(m);
    LOG.info(json);

    assertTrue("Project ID not set", json.contains("projectId"));
    assertTrue("Preemptible setting missing", json.contains("preemptible"));
    assertTrue("inputFile path wrong", json.contains("../TaskOne/output-one.txt"));
  }
}
