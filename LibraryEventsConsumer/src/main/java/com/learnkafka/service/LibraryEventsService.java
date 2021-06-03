package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventRepository;

@Service
public class LibraryEventsService {
	
	Logger logger = LogManager.getLogger(LibraryEventsService.class);
	
	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	private LibraryEventRepository libraryEventRepository;
	
	public void processLibraryEvents(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
		
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		
		logger.info("Library Events {} ",libraryEvent);
		
		switch(libraryEvent.getLibraryEventType()) {
		case NEW:
			//save Operation
			save(libraryEvent);
			break;
		case UPDATE:
			//validate Operation
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			logger.error("library event type is not valid...");
			break;
		}
		
	}

	private void validate(LibraryEvent libraryEvent) {
		
		if(libraryEvent.getLibraryEventId()==null)
			throw new IllegalArgumentException("Library Event Id should not be null");	
		
		//Manual Network Issue thrown here
		if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==111) {
			throw new RecoverableDataAccessException("111 libraryEventId");
		}
		
		Optional<LibraryEvent> getLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
		
		if(!getLibraryEvent.isPresent()) {
			throw new IllegalArgumentException("Library Event Id is not availabe to update....");	
		}
		
		logger.info("Validation is successfull for the given libraryEvent {}",libraryEvent);
	}



	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		logger.info("Library Events {} Successfully Saved..",libraryEvent);
		
	}
	
	public void recoveryProcessing(ConsumerRecord<Integer, String> consumerRecord) {
		
		Integer key = consumerRecord.key();
		String message = consumerRecord.value();
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, message);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key,message,result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				
				handleFailure(key,message,ex);
			}
		});
		
		
	}
	
	protected void handleFailure(Integer key, String value, Throwable ex) {
		
		System.out.println("Error sending the message and the exception is {}" + ex.getMessage());;
		logger.error("Error sending the message and the exception is {}",ex.getMessage());
		try {	
			throw ex;
		}
		catch(Throwable throwable) {
			logger.error("Error in OnFailure");
		}
		
	}

	protected void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		logger.info("Message sent successfully for the key :{} and the value is {},partition is {}",key,value,result.getRecordMetadata().partition());
		
	}

}
