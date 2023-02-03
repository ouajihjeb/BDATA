package org.example;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.example.Writer.CsvWriter;
import org.example.Writer.HdfsWriter;
import org.example.naissance.beans.Revenus;
import org.example.reader.CsvReader;
import org.example.reader.HdfsReader;

import java.io.IOException;

import static org.apache.spark.sql.functions.count;

/**
 * Hello world!
 *
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {


        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.appname");

        SparkSession sparkSession = SparkSession
                .builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();


        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());


        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");


        HdfsReader reader = HdfsReader.builder()
                .sparkSession(sparkSession)
                .hdfs(hdfs)
                .inputPathStr(inputPathStr)
                .build();

        HdfsWriter<Revenus> writer = new HdfsWriter<>(outPathStr);


        Dataset<Revenus> RevenusDataset = reader.get().cache();
        RevenusDataset.printSchema();
        RevenusDataset.show(5, false);

        log.info("nb Revenus = {}", RevenusDataset.count());

        writer.accept(RevenusDataset);

        log.info("done");
        //Thread.sleep(1000 * 60 * 10);





/*

        FileSystem hdfs =FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");

        RevenusStatFunction revenusStatFunction = new RevenusStatFunction();


        //Dataset<String> inputDS2 = sparkSession.read().textFile(inputPathStr);

        CsvReader csvReader = new CsvReader(sparkSession,inputPathStr, hdfs);
        CsvWriter csvWriter = new CsvWriter(outPathStr) ;


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

*/ //lahneee




//data se row je fait group by et je donne nom colonne / group by dans dtaSet objet je fait groupByKey et je fait classe funct a part
        //apres groupby je fait reduce pour applicer les aggregate




       // Dataset<Revenus> wm1 = revenusDataset.groupByKey()







    //    inputDS.select("revenu").show() ;

    //    inputDS.createOrReplaceTempView("datadeces") ;

    //   Dataset<Row> ballet = sparkSession.sql("select * from datadeces where revenu = '8208039'");
     //  ballet.show() ;





    }
}