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

import com.google.crypto.tink.subtle.Random;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


/**
 * An implementation of {@link HoodieVectorClusteringAlgorithm} that clusters the vectors randomly into a fixed number of clusters.
 */
public class HoodieRandomVectorClusteringAlgorithm extends HoodieVectorClusteringAlgorithm {

  public HoodieRandomVectorClusteringAlgorithm(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  public HoodieData<HoodieVector> cluster(HoodieEngineContext engineContext, HoodieData<HoodieVector> vectors, int numClusters) {
    JavaRDD<HoodieVector> vectorRDD = HoodieJavaRDD.getJavaRDD(vectors);
    vectorRDD = vectorRDD.mapToPair(v -> new Tuple2<>(new Integer(Random.randInt(numClusters)), v))
        .partitionBy(new PartitionIdPassthrough(numClusters))
        .map(Tuple2::_2);

    return HoodieJavaRDD.of(vectorRDD);
  }

  /**
   * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
   * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
   * NOTE: This is a workaround for SPARK-39391, which moved the PartitionIdPassthrough from
   * {@link org.apache.spark.sql.execution.ShuffledRowRDD} to {@link Partitioner}.
   */
  private static class PartitionIdPassthrough extends Partitioner {

    private final int numPartitions;

    public PartitionIdPassthrough(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      return (int) key;
    }
  }

}
