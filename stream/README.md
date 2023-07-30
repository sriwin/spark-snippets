
# Spark Scala - Streaming

## Notes

- Spark offers two different stream-processing APIs.
  - Spark Streaming 
  - Structured Streaming

### Spark Streaming
- a separate library to process continuously flowing of streaming data.
- It uses DStream API which is powered by Spark RDDs

### Structured Streaming
- built on Spark SQL library
- based on Dataframe and Dataset APIs
- uses micro-batch architecture where data is divided into batches at regular time intervals known as batch intervals
  - default time interval is 200ms
  - Each batch has N number of blocks, where N = (batch-interval/block-interval)
  - The number of blocks in the batch is equivalent to the number of tasks. An increase/decrease in blocks can increase parallelism
-  supports the following three output modes
  - outputMode("update") :
    - entire result is stored or written as output
  - outputMode("append") :
    - Only the new records in the result are written as output
  - outputMode("complete") :
    - Only the updated records in the result are written as output

## Code

### SQL Streaming Code 


```sql
create table target_table(id bigint, col_01 bigint, col_02 string) using delta;

create table source_table(id bigint, col_01 bigint, col_02 string) using delta;

insert into source_table values(1,238, "a1"),(238,2388, "b1");

CREATE SCAN source_stream_1 ON source_table USING STREAM;

CREATE STREAM table_stream
        OPTIONS(
          checkpointLocation='/tmp/table_stream/target_table'
        )
        MERGE INTO target_table AS target
        USING (select id, col_01, col_02 
               from (
                      SELECT id, col_01, col_02, row_number() over(partition by key order by value) as rn  
                      FROM source_stream_1
                    ) where rn = 1
              ) AS source
        ON target.id=source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
```


### Scala Code - Publish 2 EventHub

- Reads delta Lake Table and publishes it onto EventHub

```scala

%scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, struct, to_json}

val loadPath = "abfss://cntnr@storage.dfs.core.windows.net/folder_01/folder_01/tweets_payloads"

// 01 ::: DataFrame (streamDeltaTableDF) ::: represents an unbounded table containing the streaming text data
val streamDeltaTableDF = spark.readStream
                              .format("delta")
                              .option("maxBytesPerTrigger", 32000000)
                              .option("startingVersion", "latest")
                              .option("ignoreChanges", "true")
                              .option("ignoreDeletes", "true")
                              .load(loadPath)

// 02 ::: below code will convet all columns of a row into json which is  streaming DataFrame 
val eventHubPublisherDF = streamDeltaTableDF.select(to_json(struct(streamDeltaTableDF.columns.map{c => coalesce(col(c), lit("")).as(c)}:_*)).as("body"))

/**
 * build a dataframe on streaming data which starts streaming computation using start())
 * awaitTermination() - prevent the process from exiting while the job is active.
 */
val writeDeltaDF = eventHubPublisherDF.writeStream
                                      .format("eventhubs")
                                      .options(eventHubsConf.toMap)
                                      .option("checkpointLocation", checkpointLocation)
                                      .trigger(Trigger.ProcessingTime("5 seconds"))
                                      .start()

  writeDeltaDF.processAllAvailable()
  writeDeltaDF.stop()  
  print(writeDeltaDF)

writeDeltaDF.awaitTermination()
```

### Scala Code - Consume Events from Event Hub

```scala
```


### Java Code - Publish 2 EventHun

```java
 try {
        String envName = "";
        String eventHubName = "";
        String eventHubConxString = "";
        EventHubProducerClient producerAsync = new EventHubClientBuilder().connectionString(eventHubConxString, eventHubName).buildProducerClient();

        // 02 -
        String filePath = "c:/temp/" + "payload.csv";
        List<String> payLoadList = FileUtils.readLines(new File(filePath), StandardCharsets.UTF_8);

        //
        EventDataBatch eventHubBatch = producerAsync.createBatch();
        payLoadList.forEach(item -> {
            eventHubBatch.tryAdd(new EventData(item));
        });
        producerAsync.send(eventHubBatch);
        producerAsync.close();
        System.out.println("Payload posted successfully 2 event hub => " + eventHubName + " && total messages =  " + payLoadList.size());
} catch (Exception e) {
  throw new RuntimeException(e);
}
```
