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
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;

/** Merge branches in the graph. */
@SuppressWarnings("serial")
public class MergeBranches
    extends PTransform<
        PCollectionList<KV<String, WorkflowArgs>>, PCollection<KV<String, WorkflowArgs>>> {

  public MergeBranches() {
    super();
  }

  public MergeBranches(String name) {
    super(name);
  }

  @Override
  public PCollection<KV<String, WorkflowArgs>> apply(
      PCollectionList<KV<String, WorkflowArgs>> input) {
    return input
        .apply(Flatten.<KV<String, WorkflowArgs>>pCollections())
        .apply(Combine.globally(new Merge()));
  }

  private static class Merge
      implements SerializableFunction<
          Iterable<KV<String, WorkflowArgs>>, KV<String, WorkflowArgs>> {

    @Override
    public KV<String, WorkflowArgs> apply(Iterable<KV<String, WorkflowArgs>> input) {
      String key = null;
      WorkflowArgs retval = null;

      // Merge arguments
      for (KV<String, WorkflowArgs> kv : input) {

        // Modify a copy
        WorkflowArgs wa = new WorkflowArgs(kv.getValue());

        // First time, nothing to merge
        if (retval == null) {
          key = kv.getKey();
          retval = wa;
        // Find differences and merge
        } else {
          retval.gatherArgs(wa);
        }
      }
      return KV.of(key, retval);
    }
  }
}
