package org.example.Function.Reader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;
@Slf4j
@Builder
@AllArgsConstructor
public class read implements Supplier<Dataset<Row>> {

    private final String inputPathStr;
    private final SparkSession spark;
    @Override
    public Dataset<Row> get() {
        Dataset<Row> ds=spark.read().option("delimiter",";").option("header","true").csv(inputPathStr);
        return ds;
    }
}
