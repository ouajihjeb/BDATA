package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.StreamProcess.StreamProcess;
import org.example.Writer.CsvWriter;
import org.example.naissance.beans.Revenus;
import org.example.reader.CsvReader;

import java.io.IOException;

import static org.apache.spark.sql.functions.count;

/**
 * Hello world!
 *
 */
@Slf4j
public class StreamMain {
    public static void main(String[] args) throws InterruptedException, IOException {


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

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
                CheckPointPathStr,
                () -> {
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    javaStreamingContext.checkpoint(CheckPointPathStr);
                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        Receiver receiver = new Receiver(inputPathStr, jsc);
        StreamProcess streamProcessor = new StreamProcess(sparkSession, outPathStr);

        receiver.get().foreachRDD(streamProcessor);    //traitemenT

        jsc.start();
        jsc.awaitTerminationOrTimeout(1000 * 60 * 5);


    }



        /*
        FileSystem hdfs =FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());


        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");
        String CheckPointPathStr = config.getString("app.path.checkpoint");



        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(

                CheckPointPathStr,
                ()-> {
                    JavaStreamingContext JavaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(
                                    sparkSession.sparkContext()), new Duration(1000 * 10)
                    );

                    JavaStreamingContext.checkpoint(CheckPointPathStr);
                    return  JavaStreamingContext;
                }
        );

  Receiver receiver = new Receiver()









        RevenusStatFunction revenusStatFunction = new RevenusStatFunction();


        //Dataset<String> inputDS2 = sparkSession.read().textFile(inputPathStr);

        CsvReader csvReader = new CsvReader(sparkSession,inputPathStr, hdfs);
        CsvWriter csvWriter = new CsvWriter(outPathStr) ;

        JavaDStream<Revenus> stringJavaDStream = receiver.get();

        stringJavaDStream.foreachRDD(
                RevenusJavaRdd -> {
                    Dataset<Revenus> RevenusDataSET = sparkSession.createDataset(
                            RevenusJavaRdd.rdd(),
                            Encoders.bean(Revenus.class)
                    );

                }

        );








                }




        );


        Dataset<Row> inputDS = csvReader.get();
      //  inputDS.printSchema();
       // inputDS.show(5, false);

        Dataset<Revenus> revenusDataset = revenusStatFunction.apply(inputDS);
        revenusDataset.printSchema();
        revenusDataset.show(5, false);
       Dataset<Row> wm =  inputDS.groupBy("annee").agg(count("secteur_d_activite").as("nb")  );
        //Dataset<Revenus> wm1 = revenusStatFunction.apply(wm);
        wm.write().mode(SaveMode.Overwrite).csv(outPathStr);
     //   csvWriter.accept(revenusDataset);
       // csvWriter.accept(wm1);






//data se row je fait group by et je donne nom colonne / group by dans dtaSet objet je fait groupByKey et je fait classe funct a part
        //apres groupby je fait reduce pour applicer les aggregate




       // Dataset<Revenus> wm1 = revenusDataset.groupByKey()







    //    inputDS.select("revenu").show() ;

    //    inputDS.createOrReplaceTempView("datadeces") ;

    //   Dataset<Row> ballet = sparkSession.sql("select * from datadeces where revenu = '8208039'");
     //  ballet.show() ;


*/


    }
