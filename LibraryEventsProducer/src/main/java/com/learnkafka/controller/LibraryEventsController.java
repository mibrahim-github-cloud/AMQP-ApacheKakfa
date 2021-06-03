package com.learnkafka.controller;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.validation.Valid;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

@RestController
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	Logger logger= LogManager.getLogger(LibraryEventsController.class);

	@PostMapping(value="/v1/libraryevent", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws InterruptedException, ExecutionException, Exception
	{
		//Invoke Kafka producer
		logger.info("Before Library event producer");
		/** 
		 * Approach 1
		 * using Configured default template topic + async
		 * **/
		//libraryEventProducer.sendLibraryEvent(libraryEvent);
		
		/** 
		 * Approach 2
		 * using Configured default template topic + sync
		 * **/
		//SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		//logger.info("SendResult value is {} ",sendResult.toString());
		
		/** 
		 * Approach 3
		 * using Configured dynamic topic + async
		 * **/
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEventDynamicTopic(libraryEvent);
		logger.info("After Library event producer");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PutMapping(value="/v1/libraryevent", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws InterruptedException, ExecutionException, Exception
	{
		Optional<Integer> eventID = Optional.ofNullable(libraryEvent.getLibraryEventId());
		
		if(!eventID.isPresent())
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Library Event ID must not be null..");
			
		
		//Invoke Kafka producer
		logger.info("Before Put Library event producer");
		
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEventDynamicTopic(libraryEvent);
		logger.info("After Put Library event producer");
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
	
}
