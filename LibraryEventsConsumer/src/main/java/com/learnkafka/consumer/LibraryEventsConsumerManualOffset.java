package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;


//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String>{
	
	Logger logger = LogManager.getLogger(LibraryEventsConsumer.class);

	@Override
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
		logger.info("ConsumerRecord : {}", consumerRecord);
		acknowledgment.acknowledge();
		
	}

}
