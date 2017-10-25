package com.siemens.bt.ho.aws.kinesis.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.siemens.bt.ho.aws.kinesis.producer.KinesisMultiRecordProducer;

@RestController
public class ProducerController {

	@Autowired
	private KinesisMultiRecordProducer multiRecordProducer;
	
	@RequestMapping(value = "/sendAll", method = RequestMethod.GET)
	ResponseEntity<Map<String, String>> sendAll() {
		multiRecordProducer.produce();
		return new ResponseEntity<>(HttpStatus.OK);
		
	}
}
