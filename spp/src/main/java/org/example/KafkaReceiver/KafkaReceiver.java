package org.example.KafkaReceiver;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.TextToRevenusFunc;
import org.example.naissance.beans.Revenus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<Revenus>> {
    final List<String> topics;
    final JavaStreamingContext jsc;
    private final Map<String, Object> kafkaParams = new HashMap<String, Object>() {{        put("bootstrap.servers",
            "localhost:9092");
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", "spark-kafka-integ");
        put("auto.offset.rest", "earliest");    }};





    @Override    public JavaDStream<Revenus> get()
    {        TextToRevenusFunc stringToRevenus = new TextToRevenusFunc() ;
        JavaInputDStream<ConsumerRecord<String, String>> directDStream = KafkaUtils.createDirectStream
                (jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)

                );

        JavaDStream<Revenus> javaDStream = directDStream.map(ConsumerRecord::value).map(stringToRevenus::apply);

        return javaDStream;
    }










/*
    @Override
    public JavaDStream<Revenus> get() {
        JavaInputDStream<ConsumerRecord<Revenus,Revenus>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics,kafkaParams) );

        JavaDStream<Revenus> javaDStream=directStream.map(ConsumerRecord::value);

        return javaDStream;
    }


*/
}