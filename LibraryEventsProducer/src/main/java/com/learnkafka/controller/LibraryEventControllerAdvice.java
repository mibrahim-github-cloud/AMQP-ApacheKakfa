package com.learnkafka.controller;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class LibraryEventControllerAdvice {
	
	Logger logger = LogManager.getLogger(LibraryEventControllerAdvice.class);
	
	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<?> handleRequestBody(MethodArgumentNotValidException ex){
		
		List<FieldError> errorList = ex.getFieldErrors();
		String errorMessage = errorList.stream()
				.map(fielderror -> fielderror.getField()+" - "+fielderror.getDefaultMessage())
				.sorted()
				.collect(Collectors.joining(", "));
		
		logger.info("ErrorMessage : "+errorMessage);
		return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
		
	}

}
