package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        //создаем Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //создаем Producer-а
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //создаем Producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        //отправляем данные - асинхронно
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //срабатывает каждый раз при успешной отправке или выбрасывает исключение
                if (e == null) {
                    //данные успешно отправлены
                    logger.info("Получены новые метаданные\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("ОШИБКА: " + e);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
