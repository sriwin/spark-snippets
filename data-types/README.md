## DataType - Snippetes

```
%scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, DataType, DecimalType, IntegerType, LongType, StringType}

import java.nio.charset.StandardCharsets

import spark.implicits._

val df = Seq((1)).toDF("id")
  .withColumn("decimal_type_01", lit("11.012").cast(new DecimalType(38, 3)))
  .withColumn("decimal_type_02", lit("12.0").cast(DecimalType(20, 0)))
  .withColumn("long_type_01", lit("13.0").cast(LongType))
  .withColumn("long_type_02", lit(null).cast(LongType))
  .withColumn("integer_type_01", lit("12.0").cast(IntegerType))
  .withColumn("integer_type_02", lit(null).cast(IntegerType))
  .withColumn("string_type_01", lit("12.0").cast(StringType))
  .withColumn("string_type_02", lit(null).cast(StringType))
  .withColumn("boolean_type_01", lit(false).cast(BooleanType))
  .withColumn("boolean_type_02", lit(null).cast(BooleanType))
  .withColumn("cast2Int", lit("12").cast(StringType))
  .withColumn("cast2String", lit("10").cast(IntegerType))
  .withColumn("cast2Decimal", lit("18.123").cast(StringType))
//df.show(false)
//display(df)

val newDF = df.select(df.columns.map(c => df.col(c).cast("integer")): _*)
//newDF.show(false)

val df02 = castColumn(df, "cast2Int", IntegerType)
val df03 = castColumn(df, "cast2String", StringType)
val df04 = castColumn(df, "cast2Decimal", new DecimalType(38, 3))
df04.printSchema

def castColumn(df: DataFrame, colName: String, colType: DataType ) : DataFrame = {
  df.withColumn(colName, df(colName).cast(colType) )
}

```