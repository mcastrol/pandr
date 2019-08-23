import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // for `when`

val spark = SparkSession.builder().appName("readcsv").master("local[1]").getOrCreate()
import spark.implicits._


val dataFrameReader = spark.read

val testdf = dataFrameReader
  .option("header", "true")
  .option("inferSchema", value = true)
  .csv("/home/marcela/in/test2.csv")

testdf.show

// approach 1: using when create a new column depending of a condition when
val test_mod1 = testdf.withColumn("status",  when($"size".isNull, 1).otherwise(0))
test_mod1.show

// approach 2: using udf
val func = udf( (i:Int) => if(i.isNaN) 0 else 1 )
// create new dataframe with added column named "notempty"
val test_mod2 = testdf.withColumn("status", func($"size"))
test_mod2.show()

//create a condition to work with only not null rules
val filterCond = testdf.columns.map(x=>col(x).isNotNull).reduce(_ && _)
println(filterCond)
val filteredDf = testdf.filter(filterCond)
filteredDf.show

//select from array of columns
val col_to_select : Array[String] = Array("order_number", "size")
val colNames = col_to_select.map(name => col(name))
println(colNames)
val selected_cols = testdf.select(colNames:_*)
selected_cols.show

//create a condition of not null only with the mandadory columns and create status
val filterCondMandatory = selected_cols.columns.map(x=>col(x).isNotNull).reduce(_ && _)
println(filterCondMandatory)
val test_final = testdf.withColumn("status",  when(filterCondMandatory, 1).otherwise(0))
test_final.show






