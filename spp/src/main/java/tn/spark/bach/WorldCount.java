package tn.spark.bach;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class WorldCount {


/*
    Config config = ConfigFactory.load("application.conf");
    String masterUrl = config.getString("app.master");
    String appName = config.getString("app.appname");

    SparkSession sparkSession = SparkSession
            .builder()
            .master(masterUrl)
            .appName(appName)
            .getOrCreate();

    String inputPathStr = config.getString("app.path.input2");
    String outPathStr = config.getString("app.path.output");
    JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

    RDD<String> textFile = sparkSession.sparkContext().textFile(inputPathStr); //premie RDD cree
    JavaDoubleRDD<String,Integer> counts = textFile.flatMap(s->Arrays.asList(s.split("")).iterator()).mapToPair(world->new Tuple2)
*/
}
