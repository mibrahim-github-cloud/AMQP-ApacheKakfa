package com.learnkafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {
	
	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Spy
	ObjectMapper objectMapper = new ObjectMapper();

	@InjectMocks
	LibraryEventProducer libraryEventProducer;
	
	@SuppressWarnings("unchecked")
	@Test
	void sendLibraryEventDynamicTopic_failure() throws JsonProcessingException, InterruptedException, ExecutionException {
		//Given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(new Book());
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		
		SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
		listenableFuture.setException(new RuntimeException("Exception in topic publishing"));
		
		//When
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);
		
		
		//Then
		assertThrows(Exception.class, ()->libraryEventProducer.sendLibraryEventDynamicTopic(libraryEvent).get());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	void sendLibraryEventDynamicTopic_success() throws JsonProcessingException, InterruptedException, ExecutionException {
		//Given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(new Book());
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		
		String eventValue = objectMapper.writeValueAsString(libraryEvent);
		
		SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
		
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("library-events", libraryEvent.getLibraryEventId(), eventValue);
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(), 1, 1);
				//(new TopicPartition("library-events", 1), null, null, System.currentTimeMillis(), null, 1, 1);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		
		listenableFuture.set(sendResult);
		
		//When
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);
				
		//Then
		SendResult<Integer, String> sendResults = libraryEventProducer.sendLibraryEventDynamicTopic(libraryEvent).get();
		assertEquals(1, sendResults.getRecordMetadata().partition());
		
	}
}
