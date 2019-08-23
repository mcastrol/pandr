/*
/**
  * Created by mcastro on 15/08/19
  */
 */
/*
/**
  * Created by mcastro on 15/08/19
  */
 */
package pandr
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import java.util.Properties


case class Purchase(
                     external_id: String,
                     brand: String,
                     model: String,
                     size_scale: String,
                     size_width: String,
                     size: String,
                     order_number: String,
                     order_time: String,
                     email: String,
                     ip: String,
                     quantity: Int,
                     price: Float,
                     currency: String
                   )


//PTP: Purchases Transformation Phase. From file to DB
//done todo: input parameters shopId userId importId local/S3 inputFile_name
//todo: add exception and error if “PT-Error Reading input File" pending to define a null dataframe
//done todo: test with submit and in EMR
//done todo: add filter rows with no mandatory fields and save rows in output
//donetodo: exclude kids shoes
//done todo: exclude by not shoe_shoe
//done todo: save status properly
//todo: correct nomenclatura - use of pure function and distinguish io
//todo: test several cases.
//todo: let different types of files
//todo pending excluding by rules - excluding by filters (are cases in purchases? check the input)
//todo using custom schema and generalize rules
//    val jdbcDF2 = spark.read
//      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//    // Specifying the custom data types of the read schema
//    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")

object Purchases_TP {

  //db connection
  val Environment: String = sys.env("SSM_ENV")
  private val Url = sys.env("SSM_URL")
  val Username: String = sys.env("SSM_USER")
  val Password = sys.env("SSM_PASSWORD")
  val TableName = "raw_import_purchases"
  val connectionProperties = new Properties()
  connectionProperties.put("user", Username)
  connectionProperties.put("Password", Password)
  connectionProperties.put("driver","org.postgresql.Driver")

  //constant variables
  private val statusStates = Map("no_error"->0, "bad_data_error"->1, "ExcludedNoShopShoe"->2, "excluded_by_filter"->3,
    "excluded_by_rules"->4,"imported"->5, "excluded_kids_shoes"->7)
  private val usage = "Usage: Purchases_TP --shopId 802 --importId 99999 --userId 99999 --loc local/network --inputFile xxxx"
  private val argumentsMap = Map(
    "shopId" -> "Int",
    "importId"  -> "Int",
    "userId"  -> "Int",
    "loc"      -> "String",
    "inputFile"     -> "String"
  )
  //purchases subject type
  private val subject=1

  //for validation
  private val mandatoryColumns = Array("order_number", "order_time", "external_id", "size", "size_scale")

  /**
   * main method for purchase transformation phase
   * load purchase input file, apply rules and save in raw_import_purchases
   * Usage: Purchases_TP --shopId 802 --importId 99999 --userId 99999 --loc local/network --inputFile xxxx
   */
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.OFF)

    //create spark spark
    //val spark = SparkSession.builder().appName("purchase_tp").getOrCreate()
    val spark = SparkSession.builder()
      .appName("purchase_tp")
      .master("local[1]")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.default.parallelism", "10")
      .getOrCreate()
    import spark.implicits._

    //processing input parameters
    val arguments = new ArgumentParser(usage, argumentsMap).parseArguments(args)
    val shopId = arguments("shopId").asInstanceOf[Int]
    val importId = arguments("importId").asInstanceOf[Int]
    val userId = arguments("userId").asInstanceOf[Int]
    val loc= arguments("loc").asInstanceOf[String]
    val inputFile= arguments("inputFile").asInstanceOf[String]

    println("Shop_id="+shopId+" Import_id="+importId+" userId="+userId+" loc="+loc+" inputFile="+inputFile)

    val pathIn = loc match {
      case "local" => "in/"
      case _ => "s3n://shoesizeme-staging/uploads/imports/" + importId + "/"
    }
    val pathOut = loc match {
      case "local" => "out/"
      case _ => "s3n://shoesizeme-staging/uploads/imports/" + importId + "/output/"
    }
    //    purchaseTransformation(spark, Url, connectionProperties, pathIn, pathOut,shopId,subject,inputFile,importId,userId)
    purchaseLoad(spark, Url, connectionProperties, pathIn, pathOut,shopId,subject,inputFile,importId,userId)

  }

  def purchaseTransformation(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, pathIn:String, pathOut:String,shopId: Int, subject: Int, inputFile: String, importId: Int, userId: Int): Unit = {
    import spark.implicits._
    //read input file
    val dataFrameReader = spark.read
    val purchasesInputFile = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv(pathIn+inputFile)

    System.out.println("=== Print out schema ===")
    purchasesInputFile.printSchema()
    purchasesInputFile.show()

    //get configured mapped columns from configuration
    val colNames: Array[String] = dbgetMappingColumns(spark, Url,connectionProperties,shopId,subject)
      .select("title")
      .map(_.getString(0))
      .collect()

    //t1: remove empty columns
    val pifT1= purchasesInputFile.toDF(colNames: _*).drop("empty")

    //get virtual columns vc
    val virtualColumns = dbgetVirtualValues(spark,Url,connectionProperties,shopId,subject)

    //get the first vc
    //todo: solve the caso of more than one vc
    val virtualColumnsFirst: Row = virtualColumns.first()
    println("title: "+virtualColumnsFirst(0)+" value: "+virtualColumnsFirst(1))

    //t2: add vc
    val pifT2 = pifT1.withColumn(colName = virtualColumnsFirst(0).toString, col=lit(virtualColumnsFirst(1)))
    pifT2.show

    //t3: rule 1. div size/10
    //todo: pass to UDF
    val pifT3 = pifT2.withColumn(colName = "size", pifT1.col("size")/10)

    //t4: converting to String
    //todo: read as string normalizing the input
    val pifT4 = pifT3.withColumn(colName = "external_id",'external_id.cast("String"))

    //t5: rule 2. left pag with 0 external id
    val pifT5 = pifT4.withColumn(colName = "external_id", lpad('external_id, 9,"0"))
    pifT5.show

    //t6: rule 3 format external_id to 999.9999.9.9
    val pifT6 = pifT5.withColumn(colName = "external_id",concat('external_id.substr(0,3),lit("."),'external_id.substr(4,4),lit("."),'external_id.substr(8,1),lit("."),'external_id.substr(9,1)))

    //t7: set status of the rows  //0 ok //1 bad_data_error => one column is not null
    val pifT7=setStatusForNotMandatoryFields(pifT6, mandatoryColumns,statusStates("bad_data_error"))
    pifT7.filter($"status"===statusStates("bad_data_error")).show

    //t8: exclude kids shoes
    //todo convert to function
    var externalIdList = "'"+pifT7.select('external_id).as[String].collect.mkString("','")+"'"
    println(externalIdList)

    //add the shoe_id and gender
    var pifT8 = dbaddShoeId(spark, Url, connectionProperties, pifT7, shopId,externalIdList)
    pifT8.show


    //t9: ExcludedKidsShoes set status=7 for gender (boys:3: girls: 4, kids_unisex: 5)
    val pifT9 =  pifT8.withColumn("status",  when('gender>=3 and 'gender<=5 and 'status.isNull, statusStates("excluded_kids_shoes")).otherwise('status))

    //t10 ExcludedNoShopShoe set status=2
    val pifT10 =  pifT9.withColumn("status",  when($"shop_shoe_id".isNull and $"status".isNull,statusStates("ExcludedNoShopShoe")).otherwise('status))

    //t11 set status in no error in not flagged rows.
    val pifT11 =  pifT10.withColumn("status",  when('status isNull,statusStates("no_error")).otherwise('status))

    statusStates foreach { case (key, value) => {
      println(key + "-->" + value)
      val a = pifT11.filter('status===value).write
        .mode(SaveMode.Overwrite).partitionBy("status").format("csv").save(pathOut+key)
    }
    }

    //io: save into db
    dbinsertRawImportPurchases(Url,connectionProperties: java.util.Properties, TableName: String,pifT11, shopId: Int, importId: Int, userId: Int)

  }

  //todo find case that use size style (search size)
  def purchaseLoad(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, pathIn:String, pathOut:String,shopId: Int, subject: Int, inputFile: String, importId: Int, userId: Int): Unit = {

    //get raw data+profile_id
    //todo split in two functions and join in spark for better performance
    val ripDf=dbgetRipPlusProfileId(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, shopId: Int, importId:Int)

    ripDf.show

    //add size_id
    val ripDf_t1= dbaddSizeId(spark, Url, connectionProperties, ripDf, shopId: Int, importId:Int)

    ripDf_t1.show


  }




  //// IO GENERIC PROCEDURES ////

  /**
   * Get mapping columns of the input file from import_configuration of the shop and subject (purchase/return or porfolio)
   * @param spark spark session
   * @param Url db url
   * @param connectionProperties  connection properties: user-password & driver
   * @param shopId shopId input main procedure
   * @param subject 1 for purchases / 2 for return / 0 for portfolio
   * @return column mapping of purchase input file.
   */
  def dbgetMappingColumns(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, shopId: Int, subject: Int ): DataFrame = {
    import spark.implicits._
    val query = s"""(select title
        from import_configurations ic
          join import_file_column_options ifco on ifco.import_configuration_id=ic.id
        where ic.shop_id=$shopId
          and ic.subject=$subject
          and ifco.order>=0
          order by ifco.order) as t"""
    val colNames = spark.read.jdbc(Url, query, connectionProperties)
    colNames
  }

  /**
   * Get Virtual columns of the input file from import_configuration of the shop and subject (purchase/return or porfolio)
   * @param spark
   * @param connectionProperties
   * @param shopId
   * @param subject
   * @return
   */
  def dbgetVirtualValues(spark: SparkSession, Url: String,connectionProperties: java.util.Properties,shopId: Int,subject: Int): DataFrame = {
    import spark.implicits._
    val query = s"""(select title,virtual_value from import_configurations ic
            join import_file_column_options ifco on ifco.import_configuration_id=ic.id
            where ic.shop_id=${shopId}
            and ic.subject=$subject
            and ifco.order is null limit 1) as t """
    val virtualColumns = spark.read
      .jdbc(Url, query, connectionProperties)
    virtualColumns.show()
    virtualColumns
  }


  //  get shoe_id+gender using the external_ids
  //todo: test savinf dt in hive and join in hive https://spark.apache.org/docs/2.3.0/sql-programming-guide.html (search join)
  /**
   * add columns to input df with shop_shoe_id, shoe_id and gender correcponding to the shoe of the external_id of the shop-
   * @param spark
   * @param Url
   * @param connectionProperties
   * @param df
   * @param shopId
   * @param externalIdList
   * @return
   */
  def  dbaddShoeId(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, df: DataFrame, shopId: Int, externalIdList: String): DataFrame = {
    import spark.implicits._
    val query = s"""(select ss.shop_shoe_id as external_id, ss.id as shop_shoe_id, ss.shoe_id, s.gender
                    from shop_shoes ss
                    inner join shoes s on ss.shoe_id=s.id
                    where shop_id=$shopId and ss.shop_shoe_id in ($externalIdList)) as t"""
    val shoesIds = spark.read.jdbc(Url, query, connectionProperties)
    val dfWithShoesIds= df.join(shoesIds, df("external_id") === shoesIds("external_id"),"left_outer").drop(shoesIds.col("external_id"))
    dfWithShoesIds
  }

  def  dbgetRipPlusProfileId(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, shopId: Int, importId:Int): DataFrame = {
    import spark.implicits._
    val query = s"""(select rip.*, p.id as profile_id from raw_import_purchases rip
                    left join users u on u.client_id=$shopId and (
                        (shop_uuid=rip.order_number and shop_uuid_type=1)
                        or
                        (shop_uuid=rip.email and u.shop_uuid_type=0)
                        )
                    left join profiles p on p.user_id=u.id
                    where shop_id=$shopId and import_id=$importId and status=0) as t"""
    val df = spark.read.jdbc(Url, query, connectionProperties)
    df
  }

  def dbaddSizeId(spark: SparkSession, Url: String, connectionProperties: java.util.Properties, df:
  DataFrame, shopId: Int, importId:Int): DataFrame = {
    import spark.implicits._
    val query = s"""(select rip.id, sz.id as size_id from raw_import_purchases rip
              left join sizes sz on sz.shoe_id=rip.shoe_id and (
              (lower(rip.size_scale)='eu' and cast(sz.size_eu as text)=rip.size) or
              (lower(rip.size_scale)='uk' and cast(sz.size_uk as text)=rip.size) or
              (lower(rip.size_scale)='us' and rip.gender!=1 and cast(sz.size_us_w as text)=rip.size) or
              (lower(rip.size_scale)='us' and rip.gender=0 and cast(sz.size_us_w as text)=rip.size)
              )
            where shop_id=$shopId and import_id=$importId and status=0) as t"""
    val dfSizeId = spark.read.jdbc(Url, query, connectionProperties)
    val dfWithSizeIds= df.join(dfSizeId, df("id") === dfSizeId("id"),"left_outer").drop(dfSizeId.col("id"))
    dfWithSizeIds
  }


  ///// specific IO procedures for purchases
  //get mapping columns of shops columns
  /**
   * Save final df into target table adding output columns
   * @param Url
   * @param connectionProperties
   * @param TableName
   * @param df
   * @param shopId
   * @param importId
   * @param userId
   */
  def dbinsertRawImportPurchases(Url: String,connectionProperties: java.util.Properties, TableName: String,df: DataFrame, shopId: Int, importId: Int, userId: Int): Unit = {
    //add fixed columns
    var dfToSave = df.withColumn("shop_id", lit(shopId)).withColumn("import_id", lit(importId))
      .withColumn("user_id", lit(userId)).withColumn("created_at", lit(current_timestamp())).withColumn("updated_at", lit(current_timestamp()))
    dfToSave.show()
    val count = dfToSave.count()
    dfToSave.write
      .mode(SaveMode.Append)
      .jdbc(Url, TableName, connectionProperties)
    println("Total inserted rows in "+TableName+" "+count)
  }



  ///GENERIC functions
  /**
   * add column status for rows that don't have mandatory fields
   * @param df
   * @param mandatoryColumns
   * @param StatusBadData
   * @return
   */
  def setStatusForNotMandatoryFields(df: DataFrame, mandatoryColumns: Array[String], StatusBadData:Int): DataFrame = {
    val colNames = mandatoryColumns.map(name => col(name))
    //generate condition expression
    val filterCondMandatory = df.select(colNames:_*).columns.map(x=>col(x).isNull).reduce(_ || _)
    println(filterCondMandatory)
    //add stattus columns
    val dfWithStatus = df.withColumn("status",  when(filterCondMandatory,  StatusBadData))
    dfWithStatus
  }



}

