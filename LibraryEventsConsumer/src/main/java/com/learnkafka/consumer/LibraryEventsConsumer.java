package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learnkafka.service.LibraryEventsService;

@Component
public class LibraryEventsConsumer {
	
	@Autowired
	private LibraryEventsService libraryEventsService;
	
	Logger logger = LogManager.getLogger(LibraryEventsConsumer.class);
	
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		
		logger.info("ConsumerRecord : {}", consumerRecord);
		libraryEventsService.processLibraryEvents(consumerRecord);
	}
	

}
