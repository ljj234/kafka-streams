package com.ozoz.kafka_streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author ozoz
 * @date 2023/7/12
 **/
@Slf4j
public class FirstStreams {

    private final static String APP_ID = "first_streams_app_id";
    private final static String BOOTSTRAP_SERVER = "47.198.112.248:9092";
    private final static String SOURCE_TOPIC = "input.topic";
    private final static String TARGET_TOPIC = "input.topic";


    public static void main(String[] args) throws InterruptedException {

        // 1. create configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2. create StreamsBuild
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(
                SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                        // the process name
                        .withName("source-processor")
                        // kafka offset's rule
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST)

        )
                // use in debug case
                .peek((k, v) -> log.info("[source] value:{}", v), Named.as("pre-transform-peek"))
                .filter((k, v) -> v != null && v.length() > 5, Named.as("filter-processor"))
                .mapValues(v -> v.toUpperCase(), Named.as("map-processor"))
                .peek((k, v) -> log.info("[source] value:{}", v), Named.as("post-transform-peek"))
                .to(TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("sink-processor"));
        // 3. create topology
        final Topology topology = builder.build();
        // 4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            latch.countDown();
            log.info("the kafka streams application is graceful closed");
        }));

        // 5. start
        kafkaStreams.start();
        log.info("the kafka streams start...");
        // 6. stop(graceful)
        latch.await();


    }
}
