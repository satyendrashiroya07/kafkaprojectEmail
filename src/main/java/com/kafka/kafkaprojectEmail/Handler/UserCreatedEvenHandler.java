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
import shiroya.userEvent.UserEvent;


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


            String emailBody =
                    "Dear " + userCreatedEvent.getUserName() + ",\n\n" +
                            "Welcome to ShiroyaECom!\n\n" +
                            "We are pleased to inform you that your account has been " +
                            "successfully registered on the ShiroyaECom portal.\n\n" +
                            "You can now explore our platform and enjoy a seamless " +
                            "shopping experience.\n\n" +
                            "Your Registration Details:\n\n" +
                            "User ID: " + userCreatedEvent.getUserId() + "\n" +
                            "Registered Email: " + userCreatedEvent.getUserEmail() + "\n\n" +
                            "Thank you for joining ShiroyaECom. We appreciate your trust " +
                            "and look forward to serving you.\n\n" +
                            "Best regards,\n" +
                            "Team ShiroyaECom\n" +
                            "Customer Support";


            System.out.println("Email: " + userCreatedEvent.getUserEmail());
            emailService.sendEmail(userCreatedEvent.getUserEmail(),
                    "Welcome to ShiroyaECom – Registration Successful",
                    emailBody
            );

        }

}
