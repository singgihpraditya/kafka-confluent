package com.singgih.kafka.listener;

import com.example.kafkalistener.avro.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class UserKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(UserKafkaListener.class);

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    /**
     * Menerima dan memproses pesan dari topic Kafka yang sudah dikonfigurasi.
     * Informasi metadata record (topic, partition, offset, key) dan payload User (id, name)
     * dicatat ke log untuk keperluan observabilitas.
     *
     * @param record record Kafka yang berisi key bertipe String dan value bertipe User (Avro)
     */
    public void listen(ConsumerRecord<String, User> record) {
        User user = record.value();
        log.info("Received message - topic: {}, partition: {}, offset: {}, key: {}",
                record.topic(), record.partition(), record.offset(), record.key());
        log.info("User payload - id: {}, name: {}", user.getId(), user.getName());
    }
}
