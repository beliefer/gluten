/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.columnarbatch;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.SparkColumnarBatchUtil;

import java.util.Arrays;
import java.util.Objects;

public final class VeloxColumnarBatches {
  public static final String COMPREHENSIVE_TYPE_VELOX = "velox";

  private static boolean isVeloxBatch(ColumnarBatches.ColumnarBatchWrapper wrapper) {
    final String comprehensiveType = ColumnarBatches.getComprehensiveLightBatchType(wrapper);
    return Objects.equals(comprehensiveType, COMPREHENSIVE_TYPE_VELOX);
  }

  public static void checkVeloxBatch(ColumnarBatches.ColumnarBatchWrapper wrapper) {
    if (ColumnarBatches.isZeroColumnBatch(wrapper.getBatchType())) {
      return;
    }
    Preconditions.checkArgument(
        isVeloxBatch(wrapper),
        String.format(
            "Expected comprehensive batch type %s, but got %s",
            COMPREHENSIVE_TYPE_VELOX, ColumnarBatches.getComprehensiveLightBatchType(wrapper)));
  }

  public static ColumnarBatch toVeloxBatch(ColumnarBatches.ColumnarBatchWrapper wrapper) {
    ColumnarBatches.checkOffloaded(wrapper.getBatchType());
    if (ColumnarBatches.isZeroColumnBatch(wrapper.getBatchType())) {
      return wrapper.getBatch();
    }
    Preconditions.checkArgument(!isVeloxBatch(wrapper));
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "VeloxColumnarBatches#toVeloxBatch");
    final long handle =
        ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), wrapper);
    final long outHandle = VeloxColumnarBatchJniWrapper.create(runtime).from(handle);
    final ColumnarBatch output = ColumnarBatches.create(outHandle);

    // Follow input's reference count. This might be optimized using
    // automatic clean-up or once the extensibility of ColumnarBatch is enriched
    final long refCnt = ColumnarBatches.getRefCntLight(wrapper);
    final IndicatorVector giv = (IndicatorVector) output.column(0);
    for (long i = 0; i < (refCnt - 1); i++) {
      giv.retain();
    }

    // close the input one
    for (long i = 0; i < refCnt; i++) {
      wrapper.getBatch().close();
    }

    // Populate new vectors to input.
    SparkColumnarBatchUtil.transferVectors(output, wrapper.getBatch());

    return wrapper.getBatch();
  }

  /**
   * Check if a columnar batch is in Velox format. If not, convert it to Velox format then return.
   * If already in Velox format, return the batch directly.
   *
   * <p>Should only be used for certain conditions when unable to insert explicit to-Velox
   * transitions through query planner.
   *
   * <p>For example, used by {@link org.apache.spark.sql.execution.ColumnarCachedBatchSerializer} as
   * Spark directly calls API ColumnarCachedBatchSerializer#convertColumnarBatchToCachedBatch for
   * query plan that returns supportsColumnar=true without generating a cache-write query plan node.
   */
  public static ColumnarBatch ensureVeloxBatch(ColumnarBatches.ColumnarBatchWrapper wrapper) {
    final ColumnarBatch light =
        ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), wrapper);
    ColumnarBatches.ColumnarBatchWrapper lightWrapper = ColumnarBatches.wrapColumnarBatch(light);
    try {
      if (isVeloxBatch(lightWrapper)) {
        return light;
      }
      return toVeloxBatch(lightWrapper);
    } finally {
      ColumnarBatches.ColumnarBatchWrapper.release(lightWrapper);
    }
  }

  /**
   * Combine multiple columnar batches horizontally, assuming each of them is already offloaded.
   * Otherwise {@link UnsupportedOperationException} will be thrown.
   */
  public static ColumnarBatch compose(ColumnarBatch... batches) {
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "VeloxColumnarBatches#compose");
    final long[] handles =
        Arrays.stream(batches)
            .mapToLong(
                b -> {
                  final ColumnarBatches.ColumnarBatchWrapper wrapper =
                      ColumnarBatches.wrapColumnarBatch(b);
                  try {
                    return ColumnarBatches.getNativeHandle(
                        BackendsApiManager.getBackendName(), wrapper);
                  } finally {
                    ColumnarBatches.ColumnarBatchWrapper.release(wrapper);
                  }
                })
            .toArray();
    final long handle = VeloxColumnarBatchJniWrapper.create(runtime).compose(handles);
    return ColumnarBatches.create(handle);
  }

  /**
   * Returns a new ColumnarBatch that contains at most `limit` rows from the given batch.
   *
   * <p>If `limit >= batch.numRows()`, returns the original batch. Otherwise, copies up to `limit`
   * rows into new column vectors.
   *
   * @param batch the original batch
   * @param limit the maximum number of rows to include
   * @return a new pruned [[ColumnarBatch]] with row count = `limit`, or the original batch if no
   *     pruning is required
   */
  public static ColumnarBatch slice(
      ColumnarBatches.ColumnarBatchWrapper wrapper, int offset, int limit) {
    int totalRows = wrapper.getBatch().numRows();
    if (limit >= totalRows) {
      // No need to prune
      return wrapper.getBatch();
    } else {
      Runtime runtime =
          Runtimes.contextInstance(
              BackendsApiManager.getBackendName(), "VeloxColumnarBatches#sliceBatch");
      long nativeHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), wrapper);
      long handle = VeloxColumnarBatchJniWrapper.create(runtime).slice(nativeHandle, offset, limit);
      return ColumnarBatches.create(handle);
    }
  }
}
