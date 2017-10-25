package com.siemens.bt.ho.aws.kinesis.consumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.collect.Lists;
import com.siemens.bt.ho.aws.kinesis.handler.RecordHandler;
import com.siemens.bt.ho.aws.kinesis.producer.KinesisMultiRecordProducer;

@Component
public class KinesisConsumer {	
	
	private static final boolean SHOULD_RUN = Boolean.TRUE;
	
	@Autowired
	private RecordHandler handler;

	@PostConstruct
	public void consume() {
		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
		clientBuilder.setCredentials(new  SystemPropertiesCredentialsProvider());		
		clientBuilder.setEndpointConfiguration(new EndpointConfiguration(KinesisMultiRecordProducer.KINESIS_ENDPOINT, KinesisMultiRecordProducer.REGION));

		AmazonKinesis kinesisClient = clientBuilder.build();
		
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName( KinesisMultiRecordProducer.STREAM_NAME );
		List<Shard> shards = Lists.newArrayList();
		String exclusiveStartShardId = null;
		do {
		    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
		    DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
		    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
		    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
		        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
		    } else {
		        exclusiveStartShardId = null;
		    }
		} while ( exclusiveStartShardId != null );
		
		String shardIterator;
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(KinesisMultiRecordProducer.STREAM_NAME);
		getShardIteratorRequest.setShardId(shards.get(0).getShardId());
		getShardIteratorRequest.setShardIteratorType("LATEST");

		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();
		
		// Continuously read data records from a shard		   
		while (SHOULD_RUN) {
		  GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		  getRecordsRequest.setShardIterator(shardIterator);
		  getRecordsRequest.setLimit(25); 

		  GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);
		  
		  // Put the result into record list. The result can be empty.
		  List<Record> records = result.getRecords();
		  ExecutorService recordHandlerExecutor = Executors.newCachedThreadPool();
		  recordHandlerExecutor.execute(() -> records.forEach(record -> handler.handle(record)));		  
		  try {
		    Thread.sleep(1000);
		  } 
		  catch (InterruptedException exception) {
		    throw new RuntimeException(exception);
		  }
		  
		  shardIterator = result.getNextShardIterator();
		}
	}
	
	
//	public static void main(String[] args) {
//		KinesisConsumer consumer = new KinesisConsumer();
//		consumer.consume();
//	}
}
