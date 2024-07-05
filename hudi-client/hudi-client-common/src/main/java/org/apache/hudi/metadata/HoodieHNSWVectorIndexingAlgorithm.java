/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import com.stepstone.search.hnswlib.jna.ConcurrentIndex;
import com.stepstone.search.hnswlib.jna.Index;
import com.stepstone.search.hnswlib.jna.SpaceName;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/***
 * An algorithm to index the vectors for optimal KNN search.
 */
public class HoodieHNSWVectorIndexingAlgorithm extends HoodieVectorIndexingAlgorithm {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHNSWVectorIndexingAlgorithm.class);

  // Using a fixed 512MB memory for the queue to avoid OOM errors
  private static final int QUEUE_MAX_MEMORY_BYTES = 512 * 1024 * 1024; // 512MB

  public HoodieHNSWVectorIndexingAlgorithm(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  public List<String> index(HoodieEngineContext enginContext, HoodieData<HoodieVector> clusteredVectors, String destinationDir, HoodieTableMetaClient metadataMetaClient) {
    // Create a distributed HNSW index for the partitioned vector data
    HoodieTimer timer = HoodieTimer.start();

    enginContext.setJobStatus(this.getClass().getSimpleName(), "Counting vectors in each cluster");
    List<Long> clusterCounts = clusteredVectors.mapPartitions(it -> {
      long count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      return Collections.singletonList(count).iterator();
    }, true).collectAsList();

    // The number of threads to be used on each executor for vector indexing
    // A minimum of 2 threads is required to ensure that the producer and consumer threads are running in parallel
    final int numberOfThreads = Math.max(2, writeConfig.getMetadataConfig().getMaxCoresForVectorIndexing());
    LOG.info("Using {} threads for vector indexing", numberOfThreads);

    enginContext.setJobStatus(this.getClass().getSimpleName(), String.format("Creating %d HNSW indexes", clusterCounts.size()));
    List<String> hnswIndexPaths = clusteredVectors.mapPartitionsWithIndex((index, it) -> {
      if (clusterCounts.get(index) > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Number of vectors in partition exceeds the maximum limit of " + Integer.MAX_VALUE);
      }

      // Read the first vector to get the dimensions
      float[] firstVector = it.next().toFloatArray();

      // BlockingQueue will hold the vectors. This is required to reduce the memory footprint as a large number of vectors may need to be
      // read from the iterator
      final int numberOfItems = clusterCounts.get(index).intValue();
      final int queueLimit = QUEUE_MAX_MEMORY_BYTES / (Float.BYTES * firstVector.length + Integer.BYTES); // 8 bytes per float
      BlockingQueue<Pair<Integer, float[]>> vectorsQueue = new ArrayBlockingQueue<>(queueLimit);
      vectorsQueue.add(Pair.of(0, firstVector));

      // Initialize the index
      Index hnswIndex = new ConcurrentIndex(SpaceName.L2, firstVector.length);
      hnswIndex.initialize(numberOfItems);

      // Start the producer thread to read the vectors from the iterator and add them to the queue
      ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
      Object completionNotifier = new Object();
      executorService.submit(() -> {
        int count = 1;
        while (it.hasNext()) {
          try {
            final float[] vector = it.next().toFloatArray();
            assert vector.length != 0; // Ensure that the vector is not empty as empty vectors are used to indicate the end of data
            assert vector.length == firstVector.length; // Ensure that all vectors have the same dimensions
            vectorsQueue.put(Pair.of(count++, vector));
            if (count % 10000 == 0) {
              LOG.info(String.format("Added %d / %d vectors to the queue", count, numberOfItems));
            }
          } catch (InterruptedException e) {
            throw new HoodieException("Interrupted while adding vectors to the queue", e);
          }
        }

        // Add float arrays of length 0 to the queue to indicate that there are no more items to consume
        // Each consumer thread will consume one such item, so we need to add as many items as the number of threads
        for (int i = 0; i < numberOfThreads; i++) {
          try {
            vectorsQueue.put(Pair.of(0, new float[0]));
          } catch (InterruptedException e) {
            throw new HoodieException("Interrupted while adding vectors to the queue", e);
          }
        }

        // Signal completion
        synchronized (completionNotifier) {
          completionNotifier.notifyAll();
        }
      });

      // Start the consumer threads to read the vectors from the queue and add them to the index
      for (int i = 1; i < numberOfThreads; i++) {
        executorService.submit(() -> {
          while (true) {
            try {
              // Index the vectors in parallel
              Pair<Integer, float[]> entry = vectorsQueue.take();
              if (entry.getRight().length == 0) {
                // queue has no more items to consume
                break;
              }
              hnswIndex.addItem(entry.getRight(), entry.getLeft());
            } catch (InterruptedException e) {
              throw new HoodieException("Interrupted while reading vectors from the queue", e);
            }
          }
        });
      }

      executorService.shutdown();
      try {
        // Wait for completion
        LOG.info("Waiting for vector indexing to complete");
        synchronized (completionNotifier) {
          completionNotifier.wait();
        }

        LOG.info("Waiting for threadpool to shutdown");
        if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
          throw new HoodieException("Timeout while waiting for vector indexing to complete");
        }
      } catch (InterruptedException e) {
        throw new HoodieException("Interrupted while waiting for vector indexing to complete", e);
      }

      // Use a temporary path to save the index
      java.nio.file.Path tempPath = Paths.get(String.format("/tmp/hnsw_index_%d.bin", index));
      LOG.info("Saving HNSW index to local path " + tempPath);
      hnswIndex.save(tempPath);

      // Copy the file from temp path to the target path
      StoragePath targetPath = new StoragePath(destinationDir, String.format("hnsw_index_%d.bin", index));
      HoodieStorage storage = metadataMetaClient.getStorage();
      LOG.info("Copying HNSW index to target path " + targetPath);
      try (InputStream is = Files.newInputStream(tempPath.toFile().toPath()); OutputStream os = storage.create(targetPath, true)) {
        FileIOUtils.copy(is, os);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return Collections.singletonList(targetPath.toString()).iterator();
    }, true).collectAsList();

    LOG.info(String.format("HNSW Index created and saved in %.2f seconds", timer.endTimer() / 1000.0));
    return hnswIndexPaths;
  }
}
