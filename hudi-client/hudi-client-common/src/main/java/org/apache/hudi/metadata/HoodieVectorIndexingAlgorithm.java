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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.List;

/***
 * An algorithm to index the vectors for optimal KNN search.
 */
public abstract class HoodieVectorIndexingAlgorithm {
  protected final HoodieWriteConfig writeConfig;

  public HoodieVectorIndexingAlgorithm(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }
  
  /**
   * Index the given vectors
   *
   * @param enginContext       The engine context
   * @param clusteredVectors   The clustered vectors to index
   * @param destinationDir     The directory where to save the index
   * @param metadataMetaClient The metadata client
   * @return The list of index paths
   */
  public abstract List<String> index(HoodieEngineContext enginContext, HoodieData<HoodieVector> clusteredVectors, String destinationDir, HoodieTableMetaClient metadataMetaClient);
}
