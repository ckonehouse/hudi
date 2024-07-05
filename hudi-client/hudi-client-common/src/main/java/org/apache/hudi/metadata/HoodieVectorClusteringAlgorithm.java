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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;

/***
 * An algorithm to cluster vectors into groups
 */
public abstract class HoodieVectorClusteringAlgorithm {
  protected HoodieWriteConfig writeConfig;

  public HoodieVectorClusteringAlgorithm(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  /**
    * Cluster the given vectors into specified number of clusters
    *
    * @param engineContext The engine context
    * @param vectors       The vectors to cluster
    * @param numClusters The number of clusters
    * @return The vector clusters
    */
  public abstract HoodieData<HoodieVector> cluster(HoodieEngineContext engineContext, HoodieData<HoodieVector> vectors, int numClusters);
}
