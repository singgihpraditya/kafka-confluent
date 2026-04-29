package com.singgih.kafka.listener;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumersKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(OrdersKafkaListener.class);

    @KafkaListener(
            topics = "${kafka.topic.consumers}",
            groupId = "orders-listener-group",
            containerFactory = "ordersListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord consumer = record.value();
        log.info("[Consumer] topic: {}, partition: {}, offset: {}",
                record.topic(), record.partition(), record.offset());
        log.info("[Consumer] id: {}, product: {}, qty: {}, created_at: {}",
                consumer.get("id"), consumer.get("product"), consumer.get("qty"), consumer.get("updated_at"));
    }
}

