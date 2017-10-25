package com.siemens.bt.ho.aws.kinesis.producer;

import org.junit.Test;

import com.siemens.bt.ho.aws.kinesis.producer.KinesisMultiRecordProducer;


public class KinesisMultiRecordProducerTest {
	
	@Test
	public void testProduce() throws Exception {
		KinesisMultiRecordProducer producer = new KinesisMultiRecordProducer();
		producer.produce();
	}

}
