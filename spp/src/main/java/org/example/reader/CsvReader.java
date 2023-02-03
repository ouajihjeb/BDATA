package org.example.reader;


import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

@RequiredArgsConstructor

public class CsvReader implements Supplier<Dataset<Row>> {

    private  final SparkSession sparkSession ;
    private  final String inputPath ;
    private final FileSystem hdfs;

    @Override
    public Dataset<Row> get() {
        try {
            Path f = new Path(inputPath);
            if (hdfs.exists(f))
            {
                FileStatus[] fileStatuses = hdfs.listStatus(f);
                String []  collect = Arrays.stream(fileStatuses).filter(l -> !l.isDirectory()).map(t -> t.getPath().toString()).toArray(String[]::new);

                return sparkSession.read().option("delimiter", ";").option("header", "true").csv(collect);}
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return sparkSession.emptyDataFrame();
    }
}
