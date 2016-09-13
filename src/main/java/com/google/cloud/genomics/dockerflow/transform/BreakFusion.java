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
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/** Break Dataflow fusion. */
@SuppressWarnings("serial")
public class BreakFusion<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public BreakFusion() {}

  public BreakFusion(String name) {
    super(name);
  }

  @Override
  public PCollection<T> apply(PCollection<T> input) {
    return input
        .apply(ParDo.named("BreakFusion").of(new DummyMapFn<T>()))
        .apply(Combine.<String, T>perKey(new First<T>()))
        .apply(Values.<T>create());
  }

  static class DummyMapFn<T> extends DoFn<T, KV<String, T>> {

    @Override
    public void processElement(DoFn<T, KV<String, T>>.ProcessContext c) throws Exception {
      c.output(KV.of(String.valueOf(c.element().hashCode()), c.element()));
    }
  }

  /**
   * Return the first element. Since ordering is not guaranteed, it should be treated as effectively
   * a random element.
   */
  static class First<T> implements SerializableFunction<Iterable<T>, T> {

    @Override
    public T apply(Iterable<T> input) {
      return input.iterator() != null && input.iterator().hasNext()
          ? input.iterator().next()
          : null;
    }
  }
}
