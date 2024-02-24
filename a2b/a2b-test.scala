import org.apache.spark.sql.functions.{array, arrays_zip, col, current_timestamp, date_format, explode, expr, lit, struct, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

val schema = StructType(
  List(
    StructField("seq_nbr", StringType),
    StructField("col_01", StringType),
    StructField("col_02", StringType),
    StructField("col_03", StringType),
    StructField("col_04", StringType),
    StructField("col_05", StringType)
  )
)

val data01 = List(
  Row("473261951", "M01", "A01", "C01", "B01", "S01"),
  Row("473261952", "M02", "A02", "C01", "B01", "S01"),
  Row("473261953", "M03", "A03", "C01", "B01", "S01"),
  Row("473261954", "M04", "A04", "C01", "B01", "S01"),
  Row("473261955", "M05", "A05", "C01", "B01", "S01"),
  Row("473261956", "M06", "A06", "C06", "B06", "S06"),
  Row("473261957", "M07", "A07", "C07", "B07", "S07")
)

val data02 = List(
  Row("473261951", "M11", "A01", "C01", "B01", "S01"),
  Row("473261952", "M02", "A12", "C01", "B01", "S01"),
  Row("473261953", "M03", "A03", "C11", "B01", "S01"),
  Row("473261954", "M04", "A04", "C01", "B11", "S01"),
  Row("473261955", "M05", "A05", "C01", "B01", "S11"),
  Row("473261956", "M06", "A06", "C06", "B06", "S06"),
  Row("473261958", "M08", "A08", "C08", "B08", "S08")
)

val spark = SparkSession.builder.getOrCreate()
import spark.implicits._

val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data01), schema)
val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data02), schema)

// todo - find missing columns and then remove them from the below computations


/**
 * rename columns
 */
val renamedColumns = df2.columns.map(c => df2(c).as(s"t_$c"))
val df3 = df2.select(renamedColumns: _*)

/**
 * join dataframes => df1 & df3
 */
val joinedDF = df1.join(df3, df1.col("seq_nbr") === df3.col("t_seq_nbr"), "fullOuter")
  .select(df1("*"), df3("*"), when(df1("seq_nbr").isNull, "target-only")
    .when(df3.col("t_seq_nbr").isNull, "source-only")
    .otherwise("matched-record").alias("data_source_flag"))
  .withColumn("a2b_run_date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

/**
 * captures missing records in both dataframes (src minus tgt & tgt minus src)
 */
val df4 = joinedDF.select(joinedDF("a2b_run_date"),
  when(joinedDF("data_source_flag") === "source-only", joinedDF("seq_nbr"))
    .otherwise("null").alias("primary_key_fields_a"),
  when(joinedDF("data_source_flag") === "target-only", joinedDF("t_seq_nbr"))
    .otherwise("null").alias("primary_key_fields_b"), joinedDF("t_seq_nbr"),
  joinedDF("data_source_flag"))
  .filter ((joinedDF("data_source_flag") === "target-only") || (joinedDF("data_source_flag") === "source-only"))

// not used
//val arrayCols = joinedDF.dtypes.filter(t => t._2.startsWith("ArrayType(StructType")).map(_._1)

/**
 * find matched records only
 */
var df5 = joinedDF.filter((joinedDF("data_source_flag") === "matched-record"))

/**
  * array column of dataframe will have the below (one seq no contains all fields)
  * ----------+---------------------------------------+
  * |seq_nbr  |col                                    |
  * +---------------+---------------------------------+
  * |473261951|{473261951, 000000473261951, seq_nbr}  |
  * |473261951|{M01, M11, col_01}                     |
  * |473261951|{A01, A01, col_02}                     |
  */
val cols = df1.columns
val df6 = df5.select(col("seq_nbr"), col("data_source_flag"),
  array(cols.map(c => struct(
    col(c).alias("src_val"), 
    col("t_" + c).cast("string").alias("tgt_val"), 
    lit(c).alias("field_name"))): _*).as("array"))

/**
  * below dataframe displays the data as below (each line is a row)
  * +---------+---------+---------+-----------+
  * |seq_nbr  |src_val  |tgt_val  |field_name |
  * +---------------+-------------+-----------+
  * |473261951|473261951|473261951|seq_nbr    |
  * |473261951|M01      |M11      |col_01     |
  * |473261951|A01      |A01      |col_02     |
  */
val df7 = df6.select(col("seq_nbr"), explode(col("array")).as("col"))
//df7.show(false)

var df8 = df7.select(col("seq_nbr"),
                       col("col.src_val").as("src_val"),
                       col("col.tgt_val").as("tgt_val"),
                       col("col.field_name").as("field_name"))
//df8.show(false)

/**
  * below dataframe displays the data as below
  * +---------+-------+-------+-----------+------+
  * |seq_nbr  |src_val|tgt_val|field_name |result|
  * +---------------+-------+-------+-----+------+
  * |473261951|M01    |M11    |col_01     |fail  |
  * |473261951|A01    |A01    |col_02     |pass  |
  */
df8 = df8.filter(df8("field_name") =!= "seq_nbr")
      .withColumn("result", when(df8("src_val") === df8("tgt_val"), lit("pass"))
        .otherwise("fail"))

df8.show(false)
