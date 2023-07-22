
# Scala Code - With Column - Date & Time

```

%scala

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{add_months, col, current_date, current_timestamp, date_format, explode, expr, from_utc_timestamp, lit, monotonically_increasing_id, rand, sequence, to_date, to_timestamp, to_utc_timestamp, unix_timestamp}
import org.apache.spark.sql.types.DateType

val df01 = spark.range(1001L, 1010L)
                .withColumn("StartDateTimeEpoch", lit(1573362092000L))
                .withColumn("StartDateTimeStamp", to_utc_timestamp(to_timestamp(col("StartDateTimeEpoch") / 1000), "UTC"))
                .withColumn("StartDateTimeTruncated", unix_timestamp(col("StartDateTimeStamp").cast(DateType)) * 1000) //truncate time component by converting to Date
                .withColumn("StartTimeMillisDiff", col("StartDateTimeEpoch") - col("StartDateTimeTruncated")) //get time component in millis
                .withColumn("StartDate_NextYr", add_months(col("StartDateTimeStamp"), 12)) //add 12 months to get next year, as Date column
                .withColumn("StartDateTimeEpoch_NextYr", unix_timestamp(col("StartDate_NextYr")) * 1000 + col("StartTimeMillisDiff")) // conver Date to unix-timestamp and add the prevous calculated diff in millis
                .withColumn("StartDateTimeStamp_NextYr", to_utc_timestamp(to_timestamp(col("StartDateTimeEpoch_NextYr") / 1000), "UTC"))
                .withColumn("updated_date", to_timestamp(functions.concat(current_date - 1, lit(' '),lit("17:00:00"))))
                .withColumn("created_date",  date_format(current_timestamp(),"MM/dd/yyyy hh:mm"))
                .withColumn("updated_date", date_format(current_timestamp(),"MM/dd/yyyy hh:mm"))
                .withColumn("last_update_date", date_format(to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:00:00"))),"yyyy-MM-dd HH:mm:ss.SSSSSS"))
                .withColumn("DATE_PST_D",date_format(to_date(from_utc_timestamp(current_timestamp(),"America/Los_Angeles"),"yyyy-MM-dd HH:MM:SS"),"yyyy-MM-dd HH:MM:SS")) 
		.withColumn("created_timestamp", add_months(current_timestamp(), -1).cast("timestamp"))
                .withColumn("modified_timestamp", to_timestamp(functions.concat(current_date - 1, lit(' '), lit(runTime))))

df01.show(false)
```

# Databricks SQL Query to fetch data using timestamp

```
select *
from db_schema.table01
where UPDATED_TIMESTAMP > to_timestamp(concat((current_date()-365),' ','17:00:00'))

```
