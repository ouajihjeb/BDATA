package org.example.StreamProcess;

import com.univocity.parsers.csv.Csv;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Time;
import org.example.Writer.CsvWriter;
import org.example.naissance.beans.Revenus;

@Slf4j
@RequiredArgsConstructor
public class StreamProcess implements VoidFunction2<JavaRDD<Revenus>, Time> {
    private final SparkSession sparkSession;
    private final String outputPathStr;

    @Override
    public void call(JavaRDD<Revenus> revenusJavaRDD, Time time) throws Exception {


        long ts = System.currentTimeMillis();
        log.info("micro-batch time={} at stored in folder={}", time, ts);

        if(revenusJavaRDD.isEmpty()){
            log.info("no data found!");
           // return;
        }

        Dataset<Revenus> RevenusDataset = sparkSession.createDataset(
                revenusJavaRDD.rdd(),
                Encoders.bean(Revenus.class)
        ).cache();


        RevenusDataset.printSchema();
        RevenusDataset.show(3, false);

        log.info("nb Totel des revenus  = {}", RevenusDataset.count());


        CsvWriter writer = new CsvWriter(outputPathStr );
        writer.accept(RevenusDataset);

        RevenusDataset.unpersist();
        log.info("TRAITEMENT  TERMINEE");

    }
}

