package org.example;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Types.RevenusFileInputFormat;
import org.example.Types.RevenusLongWritable;
import org.example.Types.RevenusText;
import org.example.naissance.beans.Revenus;

import java.util.function.Supplier;


@Slf4j
@RequiredArgsConstructor
public class Receiver implements Supplier<JavaDStream<Revenus>> {



    private final String hdfsInputPathStr;

    Function<Path,Boolean> filter = hdfsPath -> {
        return hdfsPath.getName().endsWith(".csv") &&
                !hdfsPath.getName().startsWith("_") &&
                !hdfsPath.getName().endsWith(".tmp");

    };
    private final JavaStreamingContext JavaStreamingContext ;


    private final TextToRevenusFunc textToRevenus = new TextToRevenusFunc();
    private final Function<String, Revenus> mapper = textToRevenus::apply;

    @Override
    public JavaDStream<Revenus> get() {
        JavaPairInputDStream<RevenusLongWritable, RevenusText> inputDStream = JavaStreamingContext
                .fileStream(
                        hdfsInputPathStr,
                        RevenusLongWritable.class,
                        RevenusText.class,
                        RevenusFileInputFormat.class,
                        filter,
                        true
                );
        inputDStream.print();
        return inputDStream.map(t -> t._2().toString()).map(mapper);

    }

}