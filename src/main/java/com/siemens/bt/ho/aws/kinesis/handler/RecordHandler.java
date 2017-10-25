package com.siemens.bt.ho.aws.kinesis.handler;

import java.io.UnsupportedEncodingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import com.amazonaws.services.kinesis.model.Record;
import com.siemens.bt.ho.aws.kinesis.consumer.KinesisConsumer;

@Component
public class RecordHandler {

	private static final Logger LOGGER = LogManager.getLogger(KinesisConsumer.class);

	public void handle(Record record) {
		byte[] b = new byte[record.getData().remaining()];
		record.getData().get(b);
		try {
			LOGGER.info(new String(b, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
