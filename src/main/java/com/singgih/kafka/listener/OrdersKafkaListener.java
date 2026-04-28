package com.singgih.kafka.listener;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrdersKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(OrdersKafkaListener.class);

    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "orders-listener-group",
            containerFactory = "ordersListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord order = record.value();
        log.info("[Orders] topic: {}, partition: {}, offset: {}",
                record.topic(), record.partition(), record.offset());
        log.info("[Orders] id: {}, product: {}, qty: {}, created_at: {}",
                order.get("id"), order.get("product"), order.get("qty"), order.get("created_at"));
    }
}
