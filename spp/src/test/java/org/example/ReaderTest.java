package org.example;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.naissance.beans.Revenus;
import org.example.reader.CsvReader;
import org.example.reader.HdfsReader;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
public class ReaderTest {

    String input = ConfigFactory.load().getString("app.path.input");

    @Test
    public void readerTest() throws IOException {
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
        FileSystem hdfs =FileSystem.get(spark.sparkContext().hadoopConfiguration());
        HdfsReader r=new HdfsReader(spark,hdfs, input);

        Dataset<Revenus> axtual=r.get();
        assertThat(axtual)
                .isNotNull();

    }



}
