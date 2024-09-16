package org.voltdb.aggdemo.connection;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.aggdemo.MediationMessage;

import java.util.Properties;

public class KafkaConnectionFactory extends ConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectionFactory.class);

    @Override
    public Connection create(String commaDelimitedHostnames) {
        String[] hostnameArray = commaDelimitedHostnames.split(",");

        StringBuilder kafkaBrokers = new StringBuilder();
        for (int i = 0; i < hostnameArray.length; i++) {
            kafkaBrokers.append(hostnameArray[i]);
            if (i < (hostnameArray.length - 1)) {
                kafkaBrokers.append(',');
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers.toString());
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 30000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", LongSerializer.class.getName());
        props.put("value.serializer", MediationMessageSerializer.class.getName());
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

        Producer<Long, MediationMessage> producer = new KafkaProducer<>(props);

        LOGGER.info("Connected to VoltDB via Kafka");
        return (MediationMessage nextCdr) -> {
            ComplainOnErrorKafkaCallback errorCallback = new ComplainOnErrorKafkaCallback();
            ProducerRecord<Long, MediationMessage> newRecord = new ProducerRecord<>(
                    "incoming_cdrs", nextCdr.getSessionId(), nextCdr
            );

            producer.send(newRecord, errorCallback);
        };
    }
}
