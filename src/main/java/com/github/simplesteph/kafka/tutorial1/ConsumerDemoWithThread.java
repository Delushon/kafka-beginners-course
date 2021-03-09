package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();


    }

    private ConsumerDemoWithThread() {

    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Consumer-thread создан");

        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                bootstrapServers,
                groupId,
                topic
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.setName("Consumer-thread");
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("SHUTDOWN FROM HOOK");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("ВЫХОД");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Приложение остановлено", e);
        } finally {
            logger.info("Приложение закрыто");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                                String bootstrapServers,
                                String groupId,
                                String topic) {
            this.latch = latch;

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //создаем Consumer
            consumer = new KafkaConsumer<>(properties);

            //подписываем Consumer на наши топики
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //опрашиваем новые данные
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("\nKey: " + record.key() + "\n" +
                                "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset() + "\n\n\n");
                    }

                }
            } catch (WakeupException e) {
                logger.info("SHUTDOWN");
            } finally {
                consumer.close();
                //сообщаем о завершении работы с Consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //wakeUp() - специальный метод для остановки consumer.poll()
            //выбрасывает WakeUpException
            consumer.wakeup();
        }
    }
}
