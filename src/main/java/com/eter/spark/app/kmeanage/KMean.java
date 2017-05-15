package com.eter.spark.app.kmeanage;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by rusifer on 5/13/17.
 */
public class KMean {
    private static final Logger log = LoggerFactory.getLogger("KMeanAge");
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            log.error("Can't find argument for model output");
            log.debug("Actual arguments length: " + args.length);
            log.info("Use <application-name> path/to/model");
            return;
        }

        String output = args[0];

        SparkSession session = new SparkSession.Builder()
                .appName("ALS")
                .config("spark.sql.hive.metastore.version", "3.0.0")
                .config("spark.sql.hive.metastore.jars", "/usr/local/hadoop/share/hadoop/yarn/*:" +
                        "/usr/local/hadoop/share/hadoop/yarn/lib/*:" +
                        "/usr/local/hadoop/share/mapreduce/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/mapreduce/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hadoop/share/hadoop/hdfs/*:" +
                        "/usr//local/hadoop/etc/hadoop:" +
                        "/usr/local/hadoop/share/hadoop/common/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hive/lib/*:")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = session.sql("select customerid, age from customersdetail");
        JavaRDD<Vector> rdd = dataset.javaRDD().map(row -> Vectors.dense(row.getInt(1)));
        KMeans kmeans = new KMeans().setK(4).setSeed(1L);
        KMeansModel clusters  = kmeans.run(rdd.rdd());
        clusters.save(session.sparkContext(), output);
        //clusters.toPMML(session.sparkContext(), output);
    }
}
