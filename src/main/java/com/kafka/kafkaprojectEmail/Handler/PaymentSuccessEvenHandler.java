package com.kafka.kafkaprojectEmail.Handler;

import com.kafka.kafkaprojectEmail.EmailServices.EmailService;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import shiroya.orderEvent.OrderEvent;
import shiroya.paymentEvent.PaymentEvent;

@Component
@KafkaListener(topics="order-confirmed")
public class PaymentSuccessEvenHandler {

    public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private EmailService emailService;

    public PaymentSuccessEvenHandler(EmailService emailService) {
        this.emailService = emailService;
    }

    @Transactional
    @KafkaHandler
    public void handle(@Payload OrderEvent paymentCreatedEvent) {
        LOGGER.info("Received a new event: " + paymentCreatedEvent.getOrderId() + " with productId: " + paymentCreatedEvent.getProductId());

        String emailBody = "Order Details:\n" +
                "OrderId: " + paymentCreatedEvent.getOrderId() + "\n" +
                "ProductId: " + paymentCreatedEvent.getProductId() + "\n" +
                "UserId: " + paymentCreatedEvent.getUserId()+ "\n" +
                "Quantity: " + paymentCreatedEvent.getQuantity()+ "\n" +
                "Status: " + paymentCreatedEvent.getStatus();

        System.out.println("Email: " + paymentCreatedEvent.getEmail());
        emailService.sendEmail(paymentCreatedEvent.getEmail(),
                "New Order Created",
                emailBody
        );
    }
}
