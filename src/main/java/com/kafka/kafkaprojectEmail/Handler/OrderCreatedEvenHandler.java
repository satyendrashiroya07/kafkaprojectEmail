package com.kafka.kafkaprojectEmail.Handler;

import com.kafka.kafkaprojectEmail.EmailServices.EmailService;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import shiroya.orderEvent.OrderEvent;

@Component
@KafkaListener(topics="order-created")
public class OrderCreatedEvenHandler {

    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private EmailService emailService;

    public OrderCreatedEvenHandler(EmailService emailService) {
        this.emailService = emailService;
    }

    @Transactional
    @KafkaHandler
    public void handle(@Payload OrderEvent orderCreatedEvent,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messagekey) {
	LOGGER.info("Received a new event: " + orderCreatedEvent.getOrderId() + " with productId: " + orderCreatedEvent.getProductId());

    String emailBody = "Order Details:\n" +
            "OrderId: " + orderCreatedEvent.getOrderId() + "\n" +
            "ProductId: " + orderCreatedEvent.getProductId() + "\n" +
            "UserId: " + orderCreatedEvent.getUserId()+ "\n" +
            "Quantity: " + orderCreatedEvent.getQuantity()+ "\n" +
            "Status: " + orderCreatedEvent.getStatus();

        emailService.sendEmail(orderCreatedEvent.getEmail(),
                "New Order Created",
                emailBody
        );

    }
}
