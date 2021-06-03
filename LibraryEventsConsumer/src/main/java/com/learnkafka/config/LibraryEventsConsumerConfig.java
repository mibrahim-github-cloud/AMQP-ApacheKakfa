package com.learnkafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.learnkafka.service.LibraryEventsService;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

	/**
	 * Manual Consumer Offset. 1 Start Here
	 */
	Logger logger = LogManager.getLogger(LibraryEventsConsumerConfig.class);
	
	@Autowired
	LibraryEventsService libraryEventsService;
	

	ConcurrentKafkaListenerContainerFactory<?, ?> concurrentKafkaListenerContainerFactory;

	@SuppressWarnings("unchecked")
	@Autowired
	LibraryEventsConsumerConfig(ConcurrentKafkaListenerContainerFactory<?, ?> concurrentKafkaListenerContainerFactory) {

		//concurrentKafkaListenerContainerFactory.getContainerProperties().setAckMode(AckMode.MANUAL); 
		//concurrentKafkaListenerContainerFactory.setConcurrency(3);
		
		//Custom Exception Handling...
		concurrentKafkaListenerContainerFactory.setErrorHandler(((thrownException, data) -> {
            logger.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
            //if any persist process 
        }));;
        
        
        
		concurrentKafkaListenerContainerFactory.setRecoveryCallback(context->{
			
			if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
				logger.info("Recovery Callback, Data Access exception capturing... ");
				/*
				 * String[] attributes = context.attributeNames(); for(String attribute :
				 * attributes) { logger.info("Attribute Name : "+attribute);
				 * logger.info("Attribute Value : "+context.getAttribute(attribute)); }
				 */
				
				ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
				libraryEventsService.recoveryProcessing(consumerRecord);
				
			}
			else {
				logger.info("Recovery Callback, NON Data Access exception capturing... ");
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}
			
			return null;
		});
        
        concurrentKafkaListenerContainerFactory.setRetryTemplate(retryTemplate());
        
		// System.out.println("TESTREST");
		this.concurrentKafkaListenerContainerFactory = concurrentKafkaListenerContainerFactory;
	}

	private RetryTemplate retryTemplate() {
		
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy());
		
		retryTemplate.setBackOffPolicy(backOffPolicy);
		return retryTemplate;
	}

	private RetryPolicy retryPolicy() {
		
		/*
		 * SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		 * retryPolicy.setMaxAttempts(3);
		 */
		Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
		retryableExceptions.put(IllegalArgumentException.class, false);
		retryableExceptions.put(RecoverableDataAccessException.class, true);
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, retryableExceptions, true);
		return retryPolicy;
	}

	/**
	 * 1 Ends Here
	 */

	// Default Offset "BATCH" gets used.
}
