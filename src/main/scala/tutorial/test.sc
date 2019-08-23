import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

case class Pur(
                     external_id: String,
                     email: String,
                     size: String,
                     ip: String
                   )


val upper: String => String = _.toUpperCase
val checklength: String => String = _.substring(0,5)
val checklengthUDF = udf(checklength)

val spark = SparkSession.builder().appName("readcsv").master("local[1]").getOrCreate()
import spark.implicits._
Logger.getLogger("org").setLevel(Level.OFF)

//val df = Seq(
//  ("one", 2.0),
//  ("two", 1.5)).toDF("id", "val")
//println(df)
//
//var sourceDf = Seq(1000, 2000, 3000, 4000).toDF("InputGas").show()

val shop_id = 802
val import_id = 99999
val user_id = 28128331


val dataFrameReader = spark.read

val purchases_input_file = dataFrameReader
  .option("header", "true")
  .option("inferSchema", value = true)
  .csv("/home/marcela/in/purchases_input_file.csv")

//purchases_input_file.printSchema
//
//val newNames = Seq("empty", "order_number", "order_time", "external_id","empty","email", "ip", "size")
//println(newNames)
//val dfRenamed = purchases_input_file.toDF(newNames: _*)
//dfRenamed.printSchema

purchases_input_file.show

val virtual_columns = dataFrameReader
  .option("header", "true")
  .option("inferSchema", value = true)
  .csv("/home/marcela/in/virtual_columns.csv")

virtual_columns.show

val virtual_column_2 = virtual_columns.select("title").map(_.getString(0)).collect()

val col_names = Seq("empty","order_number","order_time","external_id","empty","email","ip","size")
val pif_f_1 = purchases_input_file.toDF(col_names: _*).drop("empty")

pif_f_1.printSchema()

pif_f_1.show()

// create an object using select
//val pif = pif_f_1.select(pif_f_1("external_id"),pif_f_1("email")).as[Purchase].collectAsList
//println(pif)

//how to add empty columns in the middle

//val p1 = Purchase(pif_f_1.col("external_id"),pif_f_1.col(colName="email"))
//println(p1)

//val col_to_select="col=\""+pif_f_1.columns.head+"\" cols=\""+pif_f_1.columns.tail.mkString("\",\"")+"\""

//val col_to_select="\""+pif_f_1.columns.mkString("\",\"")+"\""

val col_to_select = Array("external_id","email","size","ip")


println(col_to_select.mkString(","))

//val test = pif_f_1.selectExpr(col_to_select:_*).map(row => Pur(row(0).toString(0),row.getString(1), row.getString(2), row.getString(3)))
//
//println(test.first())



//val test_to_df = test..toDF(col_to_select:_*)
//test_to_df.show()
//val test_1 =  test.toDF(col_to_select:_*).withColumn("external_id", checklengthUDF(test.col("external_id"))).as[Pur]

//test_1.show()

//apply udf in dataset funciona ok
//val test_2 =  pif_f_1.withColumn("ip_new", checklengthUDF(pif_f_1("ip")))
//test_2.show()

//funciona ok
//val responseWithSelectedColumns = pif_f_1.select("external_id", "email")
//val typedDataset = responseWithSelectedColumns.as[Pur]
//typedDataset.show()


//val test= pif_f_1.select(${col_to_select})
//
//test.show






