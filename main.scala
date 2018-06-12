import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._

/*
Data Ingestion Stage:
- Load the ratings data itself
- Load the item data
- Load the user data
*/

// Establish a schema for the data file
val data_schema = StructType(
  StructField("user_id", IntegerType, false) ::
  StructField("item_id", IntegerType, false) ::
  StructField("rating", IntegerType, false) ::
  StructField("timestamp", IntegerType, false) ::Nil
)

// Load the data records
val data = spark.sqlContext.read.format("csv")
  .option("delimiter", "\t")
  .option("header", "false")
  .schema(data_schema)
  .load("data/ml-100k/u.data")

// Schema for Item data
val item_schema = StructType(
  StructField("movie_id", IntegerType, true) ::
  StructField("movie_title", StringType, true) ::
  StructField("release_date", StringType, true) ::
  StructField("video_release_date", StringType, true) ::
  StructField("IMDB_URL", StringType, true) ::
  StructField("unknown", StringType, true) ::
  StructField("Action", IntegerType, true) ::
  StructField("Adventure", IntegerType, true) ::
  StructField("Animation", IntegerType, true) ::
  StructField("Childrens", IntegerType, true) ::
  StructField("Comedy", IntegerType, true) ::
  StructField("Crime", IntegerType, true) ::
  StructField("Documentary", IntegerType, true) ::
  StructField("Drama", IntegerType, true) ::
  StructField("Fantasy", IntegerType, true) ::
  StructField("Film-Noir", IntegerType, true) ::
  StructField("Horror", IntegerType, true) ::
  StructField("Musical", IntegerType, true) ::
  StructField("Mystery", IntegerType, true) ::
  StructField("Romance", IntegerType, true) ::
  StructField("Sci-Fi", IntegerType, true) ::
  StructField("Thriller", IntegerType, true) ::
  StructField("War", IntegerType, true) ::
  StructField("Western", IntegerType, true) ::Nil
)

// Load item data
val item = spark.sqlContext.read.format("csv")
  .option("delimiter", "|")
  .option("header", "false")
  .schema(item_schema)
  .load("data/ml-100k/u.item")

// Schema for User
val user_schema = StructType(
  StructField("user_id", IntegerType, false) ::
  StructField("age", IntegerType, true) ::
  StructField("gender", StringType, true) ::
  StructField("occupation", StringType, true) ::
  StructField("zip_code", IntegerType, true) ::Nil
)

// Load user data
val user = spark.sqlContext.read.format("csv")
  .option("delimiter", "|")
  .option("header", "false")
  .schema(user_schema)
  .load("data/ml-100k/u.user")










// EOF
