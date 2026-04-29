package com.singgih.kafka;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.singgih.kafka.producer.UserKafkaProducer;

@SpringBootApplication
public class KafkaListenerApplication {

    /**
     * Titik masuk utama aplikasi Spring Boot.
     * Menjalankan seluruh konteks aplikasi beserta konfigurasi Kafka.
     *
     * @param args argumen baris perintah (tidak digunakan)
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaListenerApplication.class, args);
    }

    /**
     * Mengirimkan beberapa pesan contoh ke topic Kafka segera setelah aplikasi siap.
     * Digunakan untuk keperluan pengujian alur producer–consumer secara end-to-end.
     *
     * @param producer bean UserKafkaProducer yang digunakan untuk mengirim pesan
     * @return ApplicationRunner yang dieksekusi otomatis saat startup
     */
    @Bean
    public ApplicationRunner sendSampleMessages(UserKafkaProducer producer) {
        return args -> {
            producer.send(1, "Budi Santoso");
            producer.send(2, "Sari Dewi");
            producer.send(3, "Agus Pratama");
        };
    }
}

