package com.singgih.kafka.producer;

import com.example.kafkalistener.avro.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class UserKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(UserKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topic;

    /**
     * Menyuntikkan KafkaTemplate melalui constructor injection.
     *
     * @param kafkaTemplate template Kafka yang digunakan untuk mengirim pesan
     */
    public UserKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Membangun objek User Avro dari parameter yang diberikan, lalu mengirimkannya
     * ke topic Kafka secara asinkron. Hasil pengiriman (sukses/gagal) dicatat via log.
     *
     * @param id   ID unik pengguna, digunakan juga sebagai message key
     * @param name nama lengkap pengguna
     */
    public void send(int id, String name) {
        User user = User.newBuilder()
                .setId(id)
                .setName(name)
                .build();

        kafkaTemplate.send(topic, String.valueOf(id), user)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message id={}: {}", id, ex.getMessage());
                    } else {
                        log.info("Sent message — id: {}, name: {}, partition: {}, offset: {}",
                                id, name,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
