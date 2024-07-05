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

package org.apache.hudi.utilities.perf;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HoodieVectorIndexPerfTool implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieVectorIndexPerfTool.class);
  private final Config cfg;
  private final transient HoodieSparkEngineContext engineContext;
  private final List<String> results = new ArrayList<>();

  public HoodieVectorIndexPerfTool(Config cfg, HoodieSparkEngineContext engineContext) {
    this.cfg = cfg;
    this.engineContext = engineContext;
  }

  private void run() {
    HoodieTableMetaClient metaClient;
    try {
      if (cfg.clean) {
        LOG.info("Cleaning up the target base path " + cfg.targetBasePath);
        Path targetPath = new Path(cfg.targetBasePath);
        targetPath.getFileSystem(engineContext.hadoopConfiguration()).delete(targetPath, true);
      }

      // Drop the metadata table if it exists
      LOG.info("Deleting existing MDT");
      metaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath).setConf(engineContext.getStorageConf()).build();
      HoodieTableMetadataUtil.deleteMetadataTable(metaClient, engineContext, false);
    } catch (TableNotFoundException e) {
      LOG.info(String.format("Table not found at basePath %s. Creating a new table", cfg.targetBasePath));
      generateVectorDataset();
      metaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath).setConf(engineContext.getStorageConf()).build();
    } catch (IOException e) {
      LOG.error(String.format("Failed to delete table at basePath %s", cfg.targetBasePath));
      throw new RuntimeException(e);
    }

    // Bootstrap the MDT with the vector index
    HoodieTimer timer = HoodieTimer.start();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().withEnableVectorIndex(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath).withMetadataConfig(metadataConfig).build();
    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(engineContext.getStorageConf(), writeConfig, engineContext, Option.empty());
    metaClient.reloadActiveTimeline();
    addPerfResult(String.format("Vector Index created and saved in %.2f seconds", timer.endTimer() / 1000.0));

    // Get the size of the Index
    final String vectorIndexPartitionRelPath = HoodieTableMetaClient.METAFOLDER_NAME + StoragePath.SEPARATOR + MetadataPartitionType.VECTOR_INDEX.getPartitionPath();
    final StoragePath vectorIndexPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()), vectorIndexPartitionRelPath);
    HoodieStorage storage = metaClient.getStorage();
    long totalSize = 0;
    try {
      for (StoragePathInfo pathInfo : storage.listFiles(vectorIndexPath)) {
        totalSize += pathInfo.getLength();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    addPerfResult(String.format("Total size of Vector Index: %s bytes", totalSize));

    // Generate the list of vectors to search for
    //List<Vector> searchVectors = vectorData.take(1000);

    // Execute ivfflat search
    //ivfflatSearch(model, partitionedVectorData, searchVectors);

    // Execute HNSW search
    //hnswSearch(model, partitionedVectorData, searchVectors);

    // Print the results
    results.forEach(LOG::info);
  }

  /*
  private void hnswSearchIndex(KMeansModel model, JavaRDD<Vector> partitionedVectorData, List<Vector> searchVectors) {
    List<Long> partitionCounts = partitionedVectorData.mapPartitionsWithIndex((index, it) -> {
      long count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      return Collections.singletonList(count).iterator();
    }, true).collect();

    sparkSession.sparkContext().setJobGroup("hnswSearchIndex", String.format("Searching for %d vectors using HNSW", searchVectors.size()), false);
    JavaRDD<Pair<Long, Long>> timings = jssc.parallelize(searchVectors, searchVectors.size()).map(vector -> {
      final int cluster = model.predict(vector);
      // Copy the file from target to local path
      org.apache.hadoop.fs.Path sourcePath = new org.apache.hadoop.fs.Path(cfg.targetBasePath, String.format("hnsw_index_%d.bin", cluster));
      Path tempPath = Paths.get(String.format("./hnsw_index_%d.bin", cluster));
      if (!Files.exists(tempPath)) {
        LOG.info("Copying file from " + sourcePath + " to " + tempPath);
        try (OutputStream os = Files.newOutputStream(tempPath.toFile().toPath()); InputStream is = sourcePath.getFileSystem(new Configuration()).open(sourcePath)) {
          FileIOUtils.copy(is, os);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        try {
          assert Files.exists(tempPath);
          assert Files.size(tempPath) == sourcePath.getFileSystem(new Configuration()).getFileStatus(sourcePath).getLen();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // Load the index
      HoodieTimer timer = HoodieTimer.start();
      Index hnswIndex = new ConcurrentIndex(SpaceName.L2, cfg.numDimensions);
      hnswIndex.initialize(partitionCounts.get(cluster).intValue());
      hnswIndex.load(tempPath, partitionCounts.get(cluster).intValue());
      final long totalIndexLoadTime = timer.endTimer();

      // Search the index
      timer = HoodieTimer.start();
      float[] floatArray = toFloatArray(vector.toArray());
      QueryTuple result = hnswIndex.knnQuery(floatArray, cfg.numNeighbors);
      final long totalIndexSearchTime = timer.endTimer();

      // Delete the temp file
      try {
        Files.delete(tempPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      hnswIndex.clear();

      LOG.info(String.format("Found %d KNN for vector using HNSW: %s", result.getIds().length, Arrays.toString(result.getIds())));

      return Pair.of(totalIndexLoadTime, totalIndexSearchTime);
    });

    final double totalIndexLoadTime = timings.mapToDouble(Pair::getKey).sum();
    final double totalIndexSearchTime = timings.mapToDouble(Pair::getValue).sum();
    addPerfResult(String.format("Average time to load HNSW index: %.2f msec", (float)totalIndexLoadTime / searchVectors.size()));
    addPerfResult(String.format("Average time to search HNSW index: %.2f msec", (float)totalIndexSearchTime / searchVectors.size()));
  }
*/
  /*
    * Perform a search using the IVFFlat algorithm.
    * https://www.timescale.com/blog/nearest-neighbor-indexes-what-are-ivfflat-indexes-in-pgvector-and-how-do-they-work/
  */
  /*
  private void ivfflatSearch(KMeansModel model, JavaRDD<Vector> partitionedVectorData, List<Vector> searchVectors) {
    HoodieTimer timer = HoodieTimer.start();
    sparkSession.sparkContext().setJobGroup("ivfflatSearch", String.format("Searching for %d vectors using IVFFlat", searchVectors.size()), false);
    for (Vector searchVector : searchVectors) {
      final int cluster = model.predict(searchVector);
      List<Pair<Vector, Double>> clusterVectors = partitionedVectorData.mapPartitionsWithIndex((index, it) -> {
        if (index != cluster) {
          return Collections.emptyIterator();
        }

        List<Pair<Vector, Double>> vectorDistancePairList = new ArrayList<>();
        while (it.hasNext()) {
          final Vector v = it.next();
          if (v != searchVector) {
            final double distance = Vectors.sqdist(searchVector, v);
            vectorDistancePairList.add(Pair.of(v, distance));
          }
        }

        // Sort in ascending order of distance as we want to find the nearest neighbors and limit to max number of neighbors required
        return vectorDistancePairList.stream().sorted(Comparator.comparingDouble(Pair::getValue)).limit(cfg.numNeighbors).iterator();
      }, true).collect();

      // Sort again after collecting from all partitions
      clusterVectors = clusterVectors.stream().sorted(Comparator.comparingDouble(Pair::getValue)).limit(cfg.numNeighbors).collect(Collectors.toList());
      LOG.info(String.format("Found %d KNN for vector using ivfflat: %s", clusterVectors.size(), clusterVectors));
    }

    final long elapsedTime = timer.endTimer();
    addPerfResult(String.format("IVFFlat search for %d vectors in %.2f seconds", searchVectors.size(), elapsedTime / 1000.0));
    addPerfResult(String.format("IVFFlat search: Avg time to find %d nearest neighbors = %.2f seconds", cfg.numNeighbors, elapsedTime / 1000.0 / searchVectors.size()));
  }

  private JavaRDD<Vector> partition(KMeansModel model, JavaRDD<Vector> vectorData) {
    HoodieTimer timer = HoodieTimer.start();
    sparkSession.sparkContext().setJobGroup("partition", String.format("Partitioning the vector dataset into %d partitions", model.k()), false);
    JavaRDD<Vector> partitionedVectorRDD =  vectorData.keyBy(model::predict).partitionBy(new PartitionIdPassthrough(model.k())).map(t -> t._2);
    partitionedVectorRDD.cache();
    vectorData.unpersist();
    assert partitionedVectorRDD.getNumPartitions() == model.k();

    addPerfResult(String.format("Partitioned the vector dataset with %d entries in %.2f seconds", partitionedVectorRDD.count(), timer.endTimer() / 1000.0));

    List<Integer> x = partitionedVectorRDD.mapPartitionsWithIndex((index, it) -> {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      return Collections.singletonList(count).iterator();
    }, true).collect();
    addPerfResult("Partition element counts: " + x);

    return partitionedVectorRDD;
  }

  private void saveModel(KMeansModel clusters) {
    // Save and load model
    HoodieTimer timer = HoodieTimer.start();
    final String path = cfg.targetBasePath + "/KMeansModel.bin";
    sparkSession.sparkContext().setJobGroup("saveModel", "Saving model to " + path, false);
    clusters.save(jssc.sc(), path);
    addPerfResult(String.format("Saved kmeans model to %s in %.2f seconds", path, timer.endTimer() / 1000.0));

    // Load the model to confirm
    timer = HoodieTimer.start();
    KMeansModel sameModel = KMeansModel.load(jssc.sc(), path);
    addPerfResult(String.format("Loaded kmeans model from %s in %.2f seconds", path, timer.endTimer() / 1000.0));
  }

  // Evaluate clustering by computing Silhouette score
  //ClusteringEvaluator evaluator = new ClusteringEvaluator();

  //double silhouette = evaluator.evaluate(predictions);
  //System.out.println("Silhouette with squared euclidean distance = " + silhouette);

  private KMeansModel cluster(JavaRDD<Vector> vectorData) {
    HoodieTimer timer = HoodieTimer.start();
    sparkSession.sparkContext().setJobGroup("cluster", String.format("Clustering the vector dataset into %d clusters in %d iterations", cfg.numClusters, cfg.numKMeansIterations), false);
    KMeansModel model = KMeans.train(vectorData.rdd(), cfg.numClusters, cfg.numKMeansIterations);
    addPerfResult(String.format("Clustered the vector dataset into %d clusters in %d iterations in %.2f seconds", cfg.numClusters, cfg.numKMeansIterations, timer.endTimer() / 1000.0));

    for (Vector center: model.clusterCenters()) {
      LOG.debug(" Centroid: " + center);
    }

    double cost = model.computeCost(vectorData.rdd());
    addPerfResult("Clustering cost (WSSSE): " + cost);

    return model;
  }
*/

  private void generateVectorDataset() {
    final int numFiles = (int) Math.ceil((double) cfg.numVectorsToGenerate / cfg.numVectorsPerFile);
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Generating vector dataset: dimensions=%d, #vectors=%d, #files=%d", cfg.numDimensions,
        cfg.numVectorsToGenerate, numFiles));

    // Create the schema
    StructType schema = new StructType();
    schema = schema.add("partition", DataTypes.StringType, false);
    schema = schema.add("vector", DataTypes.createArrayType(DataTypes.FloatType, false), false);

    // Generate a list of random vectors
    HoodieTimer timer = HoodieTimer.start();
    List<Long> range = LongStream.range(0, numFiles).boxed().collect(Collectors.toList());
    HoodieData<Row> vectorData = engineContext.parallelize(range, range.size()).mapPartitions(itr -> {
      final int fileIndex = itr.next().intValue();
      ArrayList<Row> data = new ArrayList<>(cfg.numVectorsPerFile);
      Random rand = new Random();
      for (int i = 0; i < cfg.numVectorsPerFile; i++) {
        float[] values = new float[cfg.numDimensions];
        for (int j = 0; j < values.length; j++) {
          values[j] = rand.nextFloat();
        }

        Row row = RowFactory.create(String.format("p%d", fileIndex / cfg.numFilesPerPartition), values);
        data.add(row);
      }
      return data.iterator();
    }, true);
    vectorData.persist("MEMORY_AND_DISK");

    // Count will trigger the generation of the vectors
    final long count = vectorData.count();

    addPerfResult("Vector dataset partitions: " + vectorData.getNumPartitions());
    addPerfResult("Vector dataset count: " + count);
    addPerfResult("Vector dataset dimensions: " + cfg.numDimensions);
    addPerfResult("Vector dataset files: " + numFiles);
    addPerfResult(String.format("Generated vector dataset in %.2f seconds", timer.endTimer() / 1000.0));

    // Write the dataset to the target path
    timer = HoodieTimer.start();
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Writing vector dataset to " + cfg.targetBasePath);
    Dataset<Row> ds = engineContext.getSqlContext().createDataFrame(HoodieJavaRDD.getJavaRDD(vectorData), schema);
    ds.write().format("hudi")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        .option(DataSourceWriteOptions.TABLE_NAME().key(), "hudi_vector_index_perf_dataset")
        .option(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "huditmp.hudi_vector_index_perf")
        .mode("overwrite")
        .save(cfg.targetBasePath);
    addPerfResult(String.format("Wrote hudi vector dataset in %.2f seconds", timer.endTimer() / 1000.0));

    vectorData.unpersist();
  }

  private void addPerfResult(String result) {
    this.results.add(result);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--dest-path"}, description = "Base path for the HUDI table to use", required = true)
    public String targetBasePath;

    @Parameter(names = {"--clean"}, description = "Delete the HUDI table before running the performance test", arity = 1)
    public boolean clean = false;

    @Parameter(names = {"--num-vectors"}, description = "Number of vectors to generate for the performance test. This is used when HUDI table does not exist")
    public long numVectorsToGenerate = 0;

    @Parameter(names = {"--num-dimensions"}, description = "Number of dimensions to use for each generated vector. This is used when HUDI table does not exist")
    public int numDimensions = 0;

    @Parameter(names = {"--num-vectors-per-file"}, description = "Number of vectors to keep in each data file. This is used when HUDI table does not exist")
    public int numVectorsPerFile = 1000000;

    @Parameter(names = {"--num-files-per-partition"}, description = "Number of files to keep in each partition. This is used when HUDI table does not exist")
    public int numFilesPerPartition = 100;

    @Parameter(names = {"--num-executors"}, description = "Total number of executors to use", required = true)
    public int numExecutors;

    @Parameter(names = {"--num-neighbors"}, description = "Number of neighbors to find for each search vector")
    public int numNeighbors = 10;

    @Parameter(names = {"--hoodie-conf"}, description = "Extra Hudi options to set while writing")
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    // Parse command line config
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    // Create spark session
    SparkConf sparkConf = UtilHelpers.buildSparkConf("HoodieVectorIndexPerfTool", "yarn");
    SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();

    // Run the performance tool
    new HoodieVectorIndexPerfTool(cfg, new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext()))).run();
  }
}
