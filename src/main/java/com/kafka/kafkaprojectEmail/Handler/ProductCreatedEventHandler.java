package com.kafka.kafkaprojectEmail.Handler;

import com.kafka.kafkaprojectEmail.EmailServices.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.kafka.kafkaprojectCore.ProductCreatedEvent;
import com.kafka.kafkaprojectEmail.Error.NotRetryableException;
import com.kafka.kafkaprojectEmail.Error.RetryableException;
import com.kafka.kafkaprojectEmail.io.ProcessdEventEntity;
import com.kafka.kafkaprojectEmail.io.ProcessedEventRepository;

import jakarta.transaction.Transactional;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {
	public final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	private RestTemplate restTemplate;
	private ProcessedEventRepository processedEventRepository;
	private EmailService emailService;

	public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository, EmailService emailService) {
		this.restTemplate = restTemplate;
		this.processedEventRepository = processedEventRepository;
		this.emailService = emailService;
	}

	@Transactional
	@KafkaHandler
	public void handle(@Payload ProductCreatedEvent productCreatedEvent,
			@Header("messageId") String messageId,
			@Header(KafkaHeaders.RECEIVED_KEY) String messagekey)
	{
//		if(true) throw new NotRetryableException("An error took place. No need to consume this message again.");

		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle() + " with productId: "+productCreatedEvent.getProductId());
		LOGGER.info("Product Details: Price "+ productCreatedEvent.getPrice() + " Quantity: " + productCreatedEvent.getQuantity());

		// check if message already processed

		ProcessdEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
		if(existingRecord != null) {
			LOGGER.info("Found a duplicate record", existingRecord.getMessageId());
			return;
		}

		String requestUrl = "http://localhost:8082/product/productName";

		// exception handling
		try
		{
			ResponseEntity<String> response = restTemplate.exchange(requestUrl,HttpMethod.GET,null,String.class);
			if(response.getStatusCode().value() == HttpStatus.OK.value())
			{
				LOGGER.info("Recieved response from a remote service");
			}
		}
		catch(ResourceAccessException ex)
		{
			LOGGER.error(ex.getMessage());
			throw new RetryableException(ex);
		}
		catch(HttpServerErrorException ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}
		catch(HttpClientErrorException ex) {
			if (ex.getStatusCode() == HttpStatus.NOT_FOUND) {
				throw new NotRetryableException(ex);
			}
		}
		catch(Exception ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}

		//save data in database
		try {

			processedEventRepository.save(new ProcessdEventEntity(messageId, productCreatedEvent.getProductId()));

			String emailBody = "Product Created:\n" +
					"Title: " + productCreatedEvent.getTitle() + "\n" +
					"Price: " + productCreatedEvent.getPrice() + "\n" +
					"Quantity: " + productCreatedEvent.getQuantity();

			System.out.println("Email: "+productCreatedEvent.getEmail());
			emailService.sendEmail(
					productCreatedEvent.getEmail(),   // 👉 any email
					"New Product Created",
					emailBody
			);

		} catch(DataIntegrityViolationException ex) {
			throw new NotRetryableException(ex);

		}
	}
}
