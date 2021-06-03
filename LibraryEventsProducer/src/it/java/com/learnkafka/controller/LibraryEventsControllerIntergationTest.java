package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(topics= {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntergationTest {
	
	@Autowired
	TestRestTemplate restTemplate;
	
	private Consumer<Integer,String> consumer;
	
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@BeforeEach
	private void setUp() {
		
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
	@AfterEach
	private void tearDown()
	{
		consumer.close();
	}
	
	@Test
	@Timeout(5)
	void postLibraryEvent() {
		
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(book);
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-type", MediaType.APPLICATION_JSON_VALUE.toString());
		HttpEntity<LibraryEvent> requestEntity = new HttpEntity<LibraryEvent>(libraryEvent, headers);
		
		ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, requestEntity, LibraryEvent.class);
		
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
		
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
		String actual = consumerRecord.value();
		String expected = "{\"libraryEventId\":null,\"book\":{\"bookId\":12345,\"bookName\":\"Apache Kafka in SpringBoot\",\"bookAuthor\":\"Mohammed\"},\"libraryEventType\":\"NEW\"}";
		
		assertEquals(expected, actual);
			
	}
	
	@Test
	@Timeout(5)
	void putLibraryEvent_4xxfailure() {
		
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(book);
		libraryEvent.setLibraryEventId(85858);
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-type", MediaType.APPLICATION_JSON_VALUE.toString());
		HttpEntity<LibraryEvent> requestEntity = new HttpEntity<LibraryEvent>(libraryEvent, headers);
		
		ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, requestEntity, LibraryEvent.class);
		
		assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
		
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
		String actual = consumerRecord.value();
		String expected = "{\"libraryEventId\":85858,\"book\":{\"bookId\":12345,\"bookName\":\"Apache Kafka in SpringBoot\",\"bookAuthor\":\"Mohammed\"},\"libraryEventType\":\"UPDATE\"}";
		
		assertEquals(expected, actual);
			
	}
	

}
