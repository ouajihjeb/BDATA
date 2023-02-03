package org.example;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.KafkaReceiver.KafkaReceiver;
import org.example.Writer.CsvWriter;
import org.example.naissance.beans.Revenus;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class MainKafka {
    public static void main(String[] args) throws IOException, InterruptedException {

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.appname");

        SparkSession sparkSession = SparkSession
                .builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();


        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");
        String CheckPointPathStr = config.getString("app.path.checkpoint");


        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());


        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());


        final Function<String, Revenus> mapper = null;

        List<String> topics = null;

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(CheckPointPathStr, () -> {


                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(

                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),

                            new Duration(1000 * 10));

                    javaStreamingContext.checkpoint(CheckPointPathStr);//


                    KafkaReceiver kafkaReceiver = new KafkaReceiver(topics, javaStreamingContext);

                    JavaDStream<Revenus> prixJavaDStream = kafkaReceiver.get();

                    prixJavaDStream.foreachRDD(
                            prixJavaRDD -> {

                                log.info("batch at {}", System.currentTimeMillis());


                                Dataset<Revenus> prixDataset = SparkSession.active().createDataset(


                                        prixJavaRDD.rdd(),
                                        Encoders.bean(Revenus.class)
                                ).cache();
                                ((Dataset<?>) prixDataset).printSchema();
                                prixDataset.show(10, false);
                                log.info("le nbre est {}", prixDataset.count());
                                CsvWriter writer = new CsvWriter(outPathStr);
                                writer.accept(prixDataset);
                                prixDataset.unpersist();

                                log.info("done..........");

                            }

                    );

                    return javaStreamingContext;

                },


                sparkSession.sparkContext().hadoopConfiguration()

        );


        jsc.start();

        jsc.awaitTermination();

    }


}




