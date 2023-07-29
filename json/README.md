
# Spark Scala - Generate JSON Files

## Approach # 1

- Very simple approach

```scala
%scala

val jsonTemplateFile = "{\"isbn\": \"isbnData\",\n" + " \"author\": \n" + "  {\n" + " \"lastname\": \"authorLastNameData\",\n" + "  \"firstname\": \"authorFirstNameData\"\n" + " },\n" + "\"editor\": \n" + "   {\n" + " \"lastname\": \"editorLastNameData\",\n" + "    \"firstname\": \"editorFirstNameData\"\n" + "  },\n" + "  \"title\": \"titleData\",  \n" + "  \"category\": [\"categoryData01\", \"categoryData02\"]\n" + " }"

var jsonDataMap = Map.empty[String, String]
jsonDataMap += ("isbnData" -> System.currentTimeMillis().toString)
jsonDataMap += ("categoryData01" -> "Programming")
jsonDataMap += ("categoryData02" -> "Technology")
jsonDataMap += ("authorFirstNameData" -> "AFN")
jsonDataMap += ("editorFirstNameData" -> "EFN")
jsonDataMap += ("authorLastNameData" -> "ALN")
jsonDataMap += ("editorLastNameData" -> "ELN")
jsonDataMap += ("titleData" -> "X-Title")

var jsonFile = jsonTemplateFile
jsonDataMap.foreach(x => {
  jsonFile = jsonFile.replace(x._1, x._2)
})
println(jsonFile)
```


## Approach # 2

- Replaces the data (table row) in the json template by reading the delta lake table

```scala
%scala
    
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import java.time.LocalDateTime

import scala.collection.mutable

val schema = StructType(Seq(
  StructField("account_id", StringType),
  StructField("payload", StringType),
  StructField("created_timestamp", StringType),
  StructField("updated_timestamp", StringType)
))
val encoder = RowEncoder(schema)


val jsonTemplate = "{\"accountId\":\"account_id\", \"lastUpdatedDateTime\":\"lastUpdatedDatetimeData\", \"allData\":{    \"customerData\": { \"accountId\": \"account_id\", \"createdDate\": \"created_date\", \"createdBy\":\"created_by\",  \"updatedDate\": \"updated_date\", \"updatedBy\": \"updated_by\", \"recordEncr\": \"record_encr\"}}}"

val sql = "select * from db_schema.customer_data"
val df01 = spark.sql(sql)
val colsList:List[String] = df01.columns.toList
val df02: DataFrame = df01.select(functions.map(colsList.flatMap(c => lit(c.toLowerCase()) :: col(c.toLowerCase()) :: Nil): _*).alias("customer_data"))
//df02.show(false)

val df03 = df02.map(row => {
  try {
    // valuesMap => (customer_data, Map(account_id -> 101, col_01-> 2, col_02 -> 2, col_03 -> 0, col_04 -> 0))
    val valuesMap:Map[String,AnyVal] = row.getValuesMap[AnyVal](row.schema.fieldNames)

    // Map(account_id -> 101, col_01 -> 2, col_02 -> 2, col_03 -> 0, col_04 -> 0)
    val map1 = valuesMap.get("customer_data")
    val jsonDataMap = map1.getOrElse("account_id", "").asInstanceOf[scala.collection.immutable.Map[String, String]]
    val filteredData = jsonDataMap.filter(v => v._2 != null)
    
    // convert immutable to mutable
    //val mutableJsonDataMap1 = filteredData.to(mutable.Map[String, String])
    val mutableJsonDataMap2 = mutable.Map(filteredData.toSeq: _*)
    mutableJsonDataMap2 += ("lastUpdatedDatetimeData" -> LocalDateTime.now().toString)
    
    val acccountId = mutableJsonDataMap2.getOrElse("account_id", "")
    var jsonPayLoad = jsonTemplate
    mutableJsonDataMap2.foreach(x => {
      jsonPayLoad = jsonPayLoad.replace(x._1, x._2)
    })
    val tmp = Row.fromSeq(Seq(acccountId, jsonPayLoad, LocalDateTime.now().toString, LocalDateTime.now().toString))
    tmp

  } catch {
    case e: Exception => {
      e.printStackTrace
      print("Payload Generation Error" + e + row)
    }
    val tmp = Row.fromSeq(Seq("", e.getMessage, "", ""))
    tmp
  }
})((encoder))
println(df03.count)
df03.show(false)
```

## Approach # 3

- Convert all columns of delta lake table row to json 

```scala
%scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, struct, to_json}

val df01 = spark.sql("select * from db_schema.customer_data limit 10")
val df02 = df01.select(to_json(struct(df01.columns.map { c => coalesce(col(c), lit("")).as(c) }: _*)).as("body"))
df02.collect().foreach(rowData => {
  val body: String = rowData.getAs[String]("body")
  println(body)
})

```

## Approach # 4

- TODO - JACKSON API


## Approach # 5

- TODO - Lift API 

