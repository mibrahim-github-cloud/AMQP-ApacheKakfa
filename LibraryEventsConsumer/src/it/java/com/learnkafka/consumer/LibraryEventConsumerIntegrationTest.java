
package com.learnkafka.consumer;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventRepository;
import com.learnkafka.service.LibraryEventsService;



@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(topics= {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
, "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;
	
	@SpyBean
	LibraryEventsConsumer libraryEventConsumerSpy;
	
	@Autowired
	LibraryEventRepository libraryEventRepository;
	
	@BeforeEach
	public void setUp() {
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown() {
		libraryEventRepository.deleteAll();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void postNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		//Given
		 String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(json).get();
		
		//When
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventRepository.findAll();
		assert libraryEventList.size()==1;
		libraryEventList.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId()!=null;
			assertEquals(456, libraryEvent.getBook().getBookId());
		});
		
	}
	
	/**
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void postUpdateLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		//Given
		//Saving the new event
		String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		
		//Updating the new event
		Book updatedBook = new Book();
		updatedBook.setBookId(456);
		updatedBook.setBookName("Kafka Using Spring Boot2.x");
		updatedBook.setBookAuthor("Dilip");
		libraryEvent.setBook(updatedBook);
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		String updatedJson = objectMapper.writeValueAsString(libraryEvent);
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();
		
		//When
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));
		
		LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
		String expected = "Kafka Using Spring Boot2.x";
		assertEquals(expected, persistedLibraryEvent.getBook().getBookName());
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void postUpdateLibraryEvent_Null_LibraryEventId() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		//Given
		Integer libraryEventId = null;
		String json = " {\"libraryEventId\":"+libraryEventId+",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot2.x\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(json).get();
		
		//When
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void postUpdateLibraryEvent_Invalid_LibraryEventId() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		//Given
		Integer libraryEventId = 1452;
		String json = " {\"libraryEventId\":"+libraryEventId+",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot2.x\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		
		//When
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void postUpdateLibraryEvent_RetryPolicy() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		//Given
		Integer libraryEventId = 111;
		String json = " {\"libraryEventId\":"+libraryEventId+",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot2.x\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		
		//When
		CountDownLatch countDownLatch = new CountDownLatch(1);
		countDownLatch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(4)).processLibraryEvents(isA(ConsumerRecord.class));
		
	}
}
