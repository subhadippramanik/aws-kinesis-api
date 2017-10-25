package com.siemens.bt.ho.aws.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

@Component
public class KinesisMultiRecordProducer {
	
	private static final Logger LOGGER = LogManager.getLogger(KinesisMultiRecordProducer.class);

	public static final String STREAM_NAME = "chilliducks_kinesis_stream";

	public static final String REGION = "eu-west-1";

	public static final String KINESIS_ENDPOINT = "kinesis.eu-west-1.amazonaws.com";

	public void produce() {
		
		//What to send
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		for (int j = 0; j < 500; j++) {
		    PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
		    putRecordsRequestEntry.setData(ByteBuffer.wrap(("Hello again: " + j).getBytes()));
		    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", j));
		    putRecordsRequestEntryList.add(putRecordsRequestEntry);
		}		
		
		//Where to send
		LOGGER.info("Configuring client..");
		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
		clientBuilder.setCredentials(new  SystemPropertiesCredentialsProvider());		
		clientBuilder.setEndpointConfiguration(new EndpointConfiguration(KINESIS_ENDPOINT, REGION));
		AmazonKinesis kinesisClient = clientBuilder.build();

		//How to send
		LOGGER.info("Sending records..");
		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(STREAM_NAME);		
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

		//Error handling with retry
		while (putRecordsResult.getFailedRecordCount() > 0) {
		    final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
		    final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
		    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
		        final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
		        final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
		        if (putRecordsResultEntry.getErrorCode() != null) {
		            failedRecordsList.add(putRecordRequestEntry);
		        }
		    }
		    putRecordsRequestEntryList.clear();
		    putRecordsRequestEntryList.addAll(failedRecordsList);
		    putRecordsRequest.setRecords(putRecordsRequestEntryList);
		    putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
		}
		
		LOGGER.info("Finished sending..");
	}

//	public static void main(String[] args) {
//		KinesisMultiRecordProducer producer = new KinesisMultiRecordProducer();
//		producer.produce();
//	}

}
