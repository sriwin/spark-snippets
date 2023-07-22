# Approach # 01

> Use the below script when you want to generate the minimal data and when the few records requires update for performing quick tests

```
%scala

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{add_months, col, current_timestamp, date_format, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}

import scala.util.Random

// 
val testEnv = "stage"
val dbSchema = "shop_db" + testEnv
val createdOrUpdatedByUser = s"uat_${testEnv}"
val accountTableName = dbSchema + "." + "account"
val upsertCondition = "target.account_id = source.account_id"

val allAccountsList = List(101, 102, 103, 104, 105)

/*
 * Error => org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0: Fail to format it to '2023-07-202 10:54:13' in the new formatter. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string
*/
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

// 
val df1 = allAccountsList.toDF("account_id")
		         .withColumn("is_active", lit("A").cast(StringType)))
                         .withColumn("created_timestamp", lit(date_format(current_timestamp(), "yyyy-MM-DD HH:mm:ss")))
										 .withColumn("updated_timestamp", lit(date_format(current_timestamp(), "yyyy-MM-DD HH:mm:ss")))
val accountDeltaTable = DeltaTable.forName(spark, accountTableName)
accountDeltaTable.as("target")
          .merge(df1.alias("source"), upsertCondition)
          .whenMatched
          .updateExpr(Map("updated_timestamp" -> "source.updated_timestamp", "is_active" -> "source.is_active"))
          .execute()

```

# Approach # 02

> Use the below snipper when you want to insert data into multiple with same primary key

```
%scala

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{add_months, col, current_date, current_timestamp, date_format, explode, expr, lit, monotonically_increasing_id, sequence, to_timestamp, to_utc_timestamp, unix_timestamp}

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}


// 01 -  define variables
val tablesList = "table01,table02,table03"
val startIndex = 1001L
val endIndex = 1099L

// 02 - 
runMethodsInParallel(tablesList, startIndex, endIndex)

/*
select *
from db_schema.table01 
where UPDATED_TIMESTAMP > to_timestamp(concat((current_date()-365),' ','06:00:00'))
*/


def runMethodsInParallel(tablesList: String, start: Long, end: Long): Unit = {
  val table_list = tablesList.split(",").toList
  implicit val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(table_list.length))
  try {
    val futures = table_list.zipWithIndex.map { case (tableName, i) =>
      Future({
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"pool${i % table_list.length}")
        runDataInParallel(tableName, start, end)
      })
    }
    Await.result(Future.sequence(futures), atMost = Duration.Inf)

  } catch {
    case e: Exception =>
      e.printStackTrace()
  }
  finally {
    executionContext.shutdownNow()
  }
}

def runDataInParallel(tableName: String, start: Long, end: Long): Unit = {  
  if (tableName.equalsIgnoreCase("addDataInTable01")) {
    addDataInTable01(start, end)

  } else if (tableName.equalsIgnoreCase("addDataInTable02")) {
    addDataInTable03(start, end)

  } else if (tableName.equalsIgnoreCase("addDataInTable03")) {
    addDataInTable03(start, end)
  }
}

def addDataInTable01(start: Long, end: Long): Unit = {
  spark.sql("truncate table db_schema.addDataInTable01")
  val randomData = start to end map (x => (x.toString, getRandomScore))
  
  import spark.implicits._
  val df = spark.sparkContext.parallelize(randomData).toDF("student_id", "score")
  df.createOrReplaceTempView("table01")
 
  //
  val sql = "select cast(student_id as string) as student_id, cast(score as decimal(9,2)) as score from table01"
  val savedf = spark.sql(sql).withColumn("created_timestamp", add_months(current_timestamp(), -1).cast("timestamp")).withColumn("modified_timestamp", to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:17:00"))))

  /*
  val savedf = df.withColumn("created_timestamp", add_months(current_timestamp(), -1).cast("timestamp"))
    .withColumn("modified_timestamp", to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:00:00"))))
  */
  savedf.write.format("delta").mode(SaveMode.Append).saveAsTable("db_schema.addDataInTable01")
}

def addDataInTable02(start: Long, end: Long): Unit = {
  import spark.implicits._
  spark.sql("truncate table db_schema.addDataInTable02")
  val randomData = start to end map (x => (x.toString, getRandomScore))
  val df = spark.sparkContext.parallelize(randomData).toDF("student_id", "score")
  val savedf = df.withColumn("creation_date", date_format(add_months(current_timestamp(), -1), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("last_update_date", date_format(to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:17:00"))), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
  savedf.write.format("delta").mode(SaveMode.Append).saveAsTable("db_schema.addDataInTable02")
}

def addDataInTable03(start: Long, end: Long): Unit = {
  spark.sql("truncate table db_schema.addDataInTable03")
  val randomData = start to end map (x => (x.toString, "A", getRandomScore))
  
  import spark.implicits._
  val df = spark.sparkContext.parallelize(randomData).toDF("student_id", "status", "score")
  df.createOrReplaceTempView("table03_view")
  
  val sql = "select cast(student_id as string) as student_id, cast(status as string) as status, cast(score as decimal(18,2)) as score from table03_view"
  val savedf = spark.sql(sql).withColumn("created_date", add_months(current_timestamp(), -1).cast("timestamp")).withColumn("updated_date", to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:17:00"))))

  /*
  val savedf = df.withColumn("created_date", add_months(current_timestamp(), -1).cast("timestamp")).withColumn("updated_date", to_timestamp(functions.concat(current_date - 1, lit(' '), lit("17:00:00"))))
  */
  savedf.write.format("delta").mode(SaveMode.Append).saveAsTable("db_schema.addDataInTable03")
}

def getRandomId: String = {
  val start = 100000001
  val end = 999999999
  val rnd = new scala.util.Random
  (rnd.nextInt(end - start) + start).toString
}

def getRandomScore: Double = {
  var random = scala.util.Random
  var start = 99
  var end = 35
  (random.nextInt(end - start) + start).toDouble
}

def getRandomScore: String = {
  var random = scala.util.Random
  var start = 101
  var end = 999
  (random.nextInt(end - start) + start).toString
}

```

# approach # 03

> Create Bulk Data & save as Parquet file in storeage for testing

```
%scala

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions.{col, current_date, lit}
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.Random

val accountTypes = List("ACTIVE","EXPIRED","SUSPENDED")


val df01 = spark.range(1001L, 1999L)
      .withColumn("ACCOUNT_ID",lit(col("id")).cast(LongType))
      .withColumn("CREATION_DATE",current_date().cast(DateType))
      .withColumn("UPDATE_DATE",lit(null).cast(DateType))
      .withColumn("COL_01",lit(9999).cast(LongType))
      .withColumn("COL_02",lit(null).cast(LongType))
      .withColumn("COL_03",lit(null).cast(StringType))
      .withColumn("COL_04",lit("ABC108").cast(StringType))			
      .withColumn("COL_05",lit(accountTypes(Random.nextInt(accountTypes.size))).cast(StringType))
      .withColumn("COL_06",lit(50.5).cast(DoubleType))
      .withColumn("COL_07",lit(null).cast(DoubleType))
      .drop("id")
println("Dataframe Count = "+df01.count())

//
// blob storage gen 2 - info
val projectFolderName = "test/perf-data"
val containerName = "cntnr"
val csvFileName = "perf-data.csv"
val parquetFileName = "perf-data.parquet"
val gen2StorageAccountName ="gen2-strg"
val gen1StorageAccountName ="gen1-strg"
val gen1BlobStoragePath = s"wasbs://${containerName}@${gen1StorageAccountName}.blob.core.windows.net/${projectFolderName}"
val gen2BlobStoragePath = s"abfss://${containerName}@${gen2StorageAccountName}.dfs.core.windows.net/${projectFolderName}"
println(s"gen1BlobStoragePath = ${gen1BlobStoragePath} && gen2BlobStoragePath = ${gen2BlobStoragePath} && csvFileName = ${csvFileName}")

// Save to Table
//df01.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("table01")

// Save the parquet file in blob storage 
df01.write.mode(SaveMode.Overwrite).parquet(gen2BlobStoragePath+"/"+parquetFileName)

// Save the parquet file in blob storage 
//df01.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(csvSourceFilePath)
```


## Approach # 04

> This code will insert data in multple tables

```

%scala

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{add_months, current_timestamp, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random

// define tables names
val dbSchema = "db_schema"
val table01TableName = dbSchema+ ".table01"
val table02TableName = dbSchema+".table02"
val table03TableName = dbSchema+".table03"

// table 01 -  col names
val table01Col01Name = "t01_col_01"
val table01Col02Name = "t01_col_02"
val table01Col03Name = "t01_col_03" 

// table 02 - col names
val table02Col01Name = "t02_col_01"
val table02Col02Name = "t02_col_02" 

// table 03 - col names
val table03Col01Name = "t03_col_01"
val table03Col02Name = "t03_col_02" 

//
val table01PkColName = "t01_pk_col"
val table02PkColName = "t02_pk_col"
val table03PkColName = "t03_pk_col"

// define queries
val sqlQry01 =
  """ select t01_pk_col, t01_col_01, t01_col_02
    | from view_01""".stripMargin

val sqlQry02 =
  """ select t02_pk_col, t01_pk_col,
    |   t02_col_01, t02_col_02
    | from view_02""".stripMargin

val sqlQry03 =
  """select t03_pk_col, t02_pk_col, t01_pk_col
    | t03_col_01, t03_col_02
    | from view_01 """.stripMargin

// define schema
val table01Schema = StructType(List(
  StructField("t01_pk_col", StringType, false),
  StructField("t01_col_01", StringType, false),
  StructField("t01_col_02", StringType, false)
))

val table02Schema = StructType(List(
  StructField("t02_pk_col", StringType, false),
  StructField("t01_pk_col", StringType, false),
  StructField("t02_col_01", StringType, false),
  StructField("t02_col_02", IntegerType, false)
))

val table03Schema = StructType(List(
  StructField("t03_pk_col", StringType, false),
  StructField("t02_pk_col", StringType, false),
  StructField("t01_pk_col", StringType, false),
  StructField("t03_col_01", StringType, false),
  StructField("t03_col_02", StringType, false)
))

// truncate tables
//spark.sql("truncate table " + table03TableName)
//spark.sql("truncate table " + table02TableName)
//spark.sql("truncate table " + table01TableName)

import spark.implicits._
// table01 - gen random data
val table01Data = 1000001L to 6000001L map (x => (getId(x, "A"), getName(x, "A"), getRandomNbr, getRandomNbr))
val table01DF = spark.sparkContext
  .parallelize(table01Data)
  .toDF("t01_pk_col", "t01_col_01", "t01_col_02", "t01_col_03")
table01DF.createOrReplaceTempView("view_01")

// 
val table02Data = 1000001L to 6000001L map(x =>  (getId(x, "P"),
  getId(x, "A"), getName(x, "P"),  getRandomNbr))
val table02DF = spark.sparkContext
  .parallelize(table02Data)
  .toDF("t02_pk_col", "t01_pk_col", "t02_col_01", "t02_col_01")
table01DF.createOrReplaceTempView("view_01")

val table03Data = 1000001L to 6000001L map (x => (getId(x, "B"), getId(x, "P"),
  getId(x, "A"), getRandomNbr, getRandomNbr))

val table03DF = spark.sparkContext
  .parallelize(table03Data)
  .toDF("t03_pk_col", "t02_pk_col",  "t01_pk_col", "t02_col_01", "t02_col_01")
table03DF.createOrReplaceTempView("view_03")

/*
// save 2 table => table02
val df01 =  spark.sql(sqlQry01)
                 .withColumn("CREATED_TIMESTAMP", current_timestamp())
                 .withColumn("UPDATED_TIMESTAMP", current_timestamp())
//df01.write.format("delta").mode(SaveMode.Append).saveAsTable(table02TableName)


// save 2 table => table01
val df02 = spark.sql(sqlQry02)
                .withColumn("col_09",  lit("9").cast(IntegerType))
                            .withColumn("col_11",  lit("0").cast(IntegerType))
                            .withColumn("CREATED_TIMESTAMP", current_timestamp())
                            .withColumn("UPDATED_TIMESTAMP", current_timestamp())
                            
//df02.write.format("delta").mode(SaveMode.Append).saveAsTable(table01TableName)

// save 2 table => table04
val df03 =  spark.sql(sqlQry03)
  .withColumn("CREATED_TIMESTAMP", current_timestamp())
  .withColumn("UPDATED_TIMESTAMP", current_timestamp())
  .withColumn("start_date", add_months(current_timestamp(),-1).cast("timestamp"))
  .withColumn("end_date",add_months(current_timestamp(),11).cast("timestamp"))
//df03.write.format("delta").mode(SaveMode.Append).saveAsTable(table03TableName)
*/

def getRandomName: String = {
  val arr = Array("A", "B", "C", "D", "E", "F", "G")
  arr(Random.nextInt(arr.size))
}


def getId(id: Long, prefix: String): String = {
  prefix + "-ID-" + id
}

def getName(id: Long, prefix: String): String = {
  prefix + "-NM-" + id
}

def getRandomNbr: String = {
  var random = scala.util.Random
  var start = 101
  var end = 999
  (random.nextInt(end - start) + start).toString
}
```
