package com.onurtokat.stream;

import com.onurtokat.model.DovizKuru;
import com.onurtokat.serde.JsonDeserializer;
import com.onurtokat.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class DovizFormatter {

    private static DovizKuru dovizKuru = new DovizKuru();
    private static final Logger LOGGER = LoggerFactory.getLogger(DovizFormatter.class);

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> dovizMainStream = builder.stream("kurlar-doviz-kuru-topic");

        LOGGER.info("kurlar-doviz-kuru-topic is being read");

        KStream<String, DovizKuru> dovizFormattedStream = dovizMainStream.map((KeyValueMapper<String, String,
                KeyValue<String, DovizKuru>>) (key, value) -> {
            String[] splitted = value.split(",");
            dovizKuru.setStatus(Double.valueOf(splitted[1]));
            dovizKuru.setDovizcinsi(splitted[3]);
            dovizKuru.setBankatipi(splitted[4]);
            dovizKuru.setKurtarihi(Double.valueOf(splitted[5]));
            dovizKuru.setDovizalis(Double.valueOf(splitted[8]));
            dovizKuru.setDurumkodu(splitted[22]);
            LOGGER.info("Key: " + dovizKuru.getDovizcinsi() + "," + dovizKuru.toString());
            return new KeyValue<>(dovizKuru.getDovizcinsi(), dovizKuru);
        }).filter((key, value) -> value.getBankatipi().equals("T") && value.getDurumkodu().equals("1"));

        dovizFormattedStream.to("dovizFormatted-topic", Produced.with(Serdes.String(), new DovizKuruSerde()));
        LOGGER.info("dovizFormatted-topic has been loaded");
        Topology topology = builder.build();
        LOGGER.info("TOPOLOGY: " + topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, new StreamsConfigCreator().getConfig());
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread("shutdownHook") {
            @Override
            public void run() {
                LOGGER.info("dovizFormattedStream is being closed");
                kafkaStreams.close();
                countDownLatch.countDown();
            }
        });

        try {
            LOGGER.info("dovizFormattedStream is being started");
            kafkaStreams.start();
            countDownLatch.await();
        } catch (Throwable e) {
            LOGGER.error("Error occured when thread countdown", e);
            System.exit(1);
        }
        LOGGER.info("dovizFormattedStream is being closing");
        System.exit(0);
    }

    public static final class DovizKuruSerde extends Serdes.WrapperSerde<DovizKuru> {
        public DovizKuruSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(DovizKuru.class));
        }
    }
}
