package com.singgih.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${kafka.security.sasl.mechanism:GSSAPI}")
    private String saslMechanism;

    @Value("${kafka.security.sasl.kerberos-service-name:kafka}")
    private String kerberosServiceName;

    @Value("${kafka.security.keytab-path:}")
    private String keytabPath;

    @Value("${kafka.security.principal:}")
    private String principal;

    /**
     * Membuat dan mengkonfigurasi ProducerFactory untuk mengirim pesan Avro ke Kafka.
     * Key menggunakan StringSerializer, value menggunakan KafkaAvroSerializer yang terhubung ke Schema Registry.
     *
     * @return ProducerFactory yang siap digunakan oleh KafkaTemplate
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        if ("SASL_PLAINTEXT".equals(securityProtocol) || "SASL_SSL".equals(securityProtocol)) {
            props.put("security.protocol", securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, kerberosServiceName);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig());
        }

        return new DefaultKafkaProducerFactory<>(props);
    }

    private String buildJaasConfig() {
        return String.format(
            "com.sun.security.auth.module.Krb5LoginModule required " +
            "useKeyTab=true storeKey=true keyTab=\"%s\" principal=\"%s\";",
            keytabPath, principal);
    }

    /**
     * Membuat KafkaTemplate sebagai bean utama untuk mengirim pesan ke topic Kafka.
     * Menggunakan ProducerFactory yang sudah dikonfigurasi dengan serializer Avro.
     *
     * @return KafkaTemplate siap pakai dengan konfigurasi producer
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
