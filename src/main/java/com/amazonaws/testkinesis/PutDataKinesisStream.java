package com.amazonaws.testkinesis;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import java.nio.ByteBuffer;
import java.util.List;

public class PutDataKinesisStream {
    static AWSCredentials credentials = null;
    public static void main(String args[]) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIA2TRSLU5GIUZ6PIEY", "aPCVOWpimpGS9ZcJcE3GdCztcXXLhYvRhvdegE21");
        AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder
                .standard().withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
        String myData = "My Data"; //can be json
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName("testJava"); //name of aws stream you created
        putRecordRequest.setPartitionKey("session" + "S1");
        putRecordRequest.withData(ByteBuffer.wrap(myData.getBytes()));
        PutRecordResult putRecordResult = amazonKinesis.putRecord(putRecordRequest);
        System.out.println(putRecordResult.getSequenceNumber());
        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName("testJava");
        getShardIteratorRequest.setShardId("shardId-000000000000");
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
        GetShardIteratorResult getShardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();
        // Continuously read data records from a shard
        List<Record> records;
        while (true) {
             // Create a new getRecordsRequest with an existing shardIterator
            // Set the maximum records to return to 25
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(25);
            GetRecordsResult result = amazonKinesis.getRecords(getRecordsRequest);
            // Put the result into record list. The result can be empty.
            records = result.getRecords();
            for (Record r : records) {
                System.out.println(r.getSequenceNumber());
                System.out.println(r.getPartitionKey());
                byte[] bytes = r.getData().array();
                System.out.println(new String(bytes));
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            }
            shardIterator = result.getNextShardIterator();
        }
    }
}