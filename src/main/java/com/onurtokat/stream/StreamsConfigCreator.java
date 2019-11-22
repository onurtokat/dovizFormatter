package com.onurtokat.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsConfigCreator {

    private Properties config = new Properties();
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsConfigCreator.class);

    public StreamsConfigCreator() {
        LOGGER.info("StreamsConfigCreator has been invoked");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "doviz-app");
        //config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        //       "fbtstcld03.fibabanka.local:9092,fbtstcld04.fibabanka.local:9092,fbtstcld05.fibabanka.local:9092");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fbdevbigd02.fibabanka.local:9092," +
                "fbdevbigd01.fibabanka.local:9092,fbdevbigd03.fibabanka.local:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //security properties
        config.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put("sasl.kerberos.service.name", "kafka");
        config.put("sasl.mechanism", "GSSAPI");
    }

    public Properties getConfig() {
        return config;
    }
}
