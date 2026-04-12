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
import productEvent.userEvent.UserEvent;


@Component
@KafkaListener(topics="user-created")
public class UserCreatedEvenHandler {

        public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
        private EmailService emailService;

        public UserCreatedEvenHandler(EmailService emailService) {
            this.emailService = emailService;
        }

        @Transactional
        @KafkaHandler
        public void handle(@Payload UserEvent userCreatedEvent,
                           @Header(KafkaHeaders.RECEIVED_KEY) String messagekey) {
            LOGGER.info("Received a new event: " + userCreatedEvent.getId() + " with Email: " + userCreatedEvent.getUserEmail());

            String emailBody = "User Created Successfully:\n" +
                    "Name: " + userCreatedEvent.getUserName() +"\n"+
                    "UserId:" + userCreatedEvent.getUserId();
            ;

            System.out.println("Email: " + userCreatedEvent.getUserEmail());
            emailService.sendEmail(userCreatedEvent.getUserEmail(),
                    "Created User",
                    emailBody
            );

        }

}
