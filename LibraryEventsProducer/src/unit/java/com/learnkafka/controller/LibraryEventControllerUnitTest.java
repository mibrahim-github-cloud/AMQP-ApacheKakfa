package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {
	
	@Autowired
	MockMvc mockMvc;
	
	ObjectMapper objectMapper = new ObjectMapper();
	
	@MockBean
	LibraryEventProducer libraryEventProducer;
	
	@Test
	void postLibraryEvent() throws Exception {
		
		//given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(book);
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		
		String json = objectMapper.writeValueAsString(libraryEvent);
		//doNothing().when(libraryEventProducer).sendLibraryEventDynamicTopic(isA(LibraryEvent.class));
		when(libraryEventProducer.sendLibraryEventDynamicTopic(isA(LibraryEvent.class))).thenReturn(null);
		
		mockMvc.perform(post("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
		.andExpect(status().isCreated());
		
	}
	
	@Test
	void postLibraryEvent_4xxClientError() throws Exception {
		
		//given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
		
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(new Book());
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		
		String json = objectMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEventDynamicTopic(isA(LibraryEvent.class))).thenReturn(null);
				
		//Expected
		String expected = "book.bookAuthor - must not be blank, book.bookId - must not be null, book.bookName - must not be blank";
		mockMvc.perform(post("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
		.andExpect(status().is4xxClientError())
		.andExpect(content().string(expected));
		
	}
	
	@Test
	void putLibraryEvent() throws Exception {
		
		//given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
				
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(book);
		libraryEvent.setLibraryEventId(14526);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);	
		
		//When
		when(libraryEventProducer.sendLibraryEventDynamicTopic(isA(LibraryEvent.class))).thenReturn(null);
		
		//Expected
		String payload = objectMapper.writeValueAsString(libraryEvent);
		mockMvc.perform(put("/v1/libraryevent")
				.content(payload)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
		.andExpect(status().isOk());
			
	}
	
	@Test
	void putLibraryEvent_4xxClientError() throws Exception {
		
		//given
		Book book = new Book();
		book.setBookId(12345);
		book.setBookName("Apache Kafka in SpringBoot");
		book.setBookAuthor("Mohammed");
				
		LibraryEvent libraryEvent = new LibraryEvent();
		libraryEvent.setBook(book);
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);	
		
		//When
		when(libraryEventProducer.sendLibraryEventDynamicTopic(isA(LibraryEvent.class))).thenReturn(null);
		
		//Expected
		String expected = "Library Event ID must not be null..";
		String payload = objectMapper.writeValueAsString(libraryEvent);
		mockMvc.perform(put("/v1/libraryevent")
				.content(payload)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
		.andExpect(status().isBadRequest())
		.andExpect(content().string(expected));
		
			
	}

	
}
