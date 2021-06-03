package com.learnkafka.producer;



import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;



@Component
public class LibraryEventProducer {
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	Logger logger = LogManager.getLogger(LibraryEventProducer.class);
	String topic = "library-events";
	
	@Autowired
	ObjectMapper objectMapper;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException
	{
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key,value,result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				
				handleFailure(key,value,ex);
			}
		});
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException,ExecutionException,Exception {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get(3, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			logger.error("ExecutionException/InterruptedException sending the message and the exception is {}",e.getMessage());
			throw e;	
		} catch(Exception e) {
			logger.error("Exception sending the message and the exception is {}",e.getMessage());
			throw e;
		}
		
		return sendResult;
		
	}
	
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEventDynamicTopic(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		

		/** Building Producer Record **/
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
				
			}
		});
		
		return listenableFuture;
		
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic){
		
		List<Header> recordHeaders = new ArrayList<>();
		recordHeaders.add(new RecordHeader("event-source", "scanner".getBytes())); 
		
		return new ProducerRecord<Integer, String>("library-events", null, key, value, recordHeaders);
				
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
