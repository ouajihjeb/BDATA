package org.example.reader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.RevenusMapper;
import org.example.naissance.beans.Revenus;

import java.io.IOException;
import java.util.function.Supplier;

@Slf4j
@Builder
@AllArgsConstructor
public class HdfsReader implements Supplier<Dataset<Revenus>> {
    private SparkSession sparkSession;
    private FileSystem hdfs;
    private String inputPathStr;

    private final RevenusMapper RevenusMapper = new RevenusMapper();

    @Override
    public Dataset<Revenus> get() {
        try {
            if(hdfs.exists(new Path(inputPathStr))) {
                Dataset<Row> rowDataset = sparkSession.read().option("delimiter", ";").option("header", "true").csv(inputPathStr);
                rowDataset.printSchema();
                rowDataset.show(5, false);


                return RevenusMapper.apply(rowDataset.toDF());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sparkSession.emptyDataset(Encoders.bean(Revenus.class));
    }
}