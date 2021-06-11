import org.apache.log4j.Logger
import java.sql.Connection
import com.my.utils.Utilities
import com.my.config.EnvPropertiesObject
import com.my.config.PropertiesObject
import com.my.config.ConfigObjectNonStreaming
import com.my.config.SetUpConfigurationNonStreaming
import com.my.config.AuditLoadObject
import com.my.config.StreamingPropertiesObject
import java.util.HashMap
import scala.collection.Map
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import com.my.utils.DataQuality
import com.my.config.SKeyObject
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.AnalysisException
import java.net.ConnectException
import org.apache.spark.sql.Row
import com.my.utils._
import com.my.config._
import com.github.opendevl.JFlat //Newly added

object AribaProcessor {

  //Initialized Log
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args == null || args.isEmpty) {
      println("Invalid number of arguments passed.")
      println("Arguments Usage: <Properties file path> <Batch Id - yyyyMMddHHmmss>")
      println("Stopping the flow")
      System.exit(1)
    }

    val propertiesFilePath = String.valueOf(args(0).trim())
    //val batchId = String.valueOf(args(1).trim())
    var offsetColumnName = "intgtn_fbrc_msg_id"
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val propertiesObject: StreamingPropertiesObject = Utilities.getStreamingPropertiesobject(propertiesFilePath)
    val configObject: ConfigObjectNonStreaming = SetUpConfigurationNonStreaming.setup()
    val sKeyFilePath=propertiesFilePath.substring(0,propertiesFilePath.lastIndexOf("/")+1)+"sKey"
    val sk:SKeyObject = Utilities.getSKeyPropertiesobject(sKeyFilePath)
    val envPropertiesFilePath=propertiesFilePath.substring(0,propertiesFilePath.lastIndexOf("/")+1)+"connection.properties"
    val sqlPropertyObject: EnvPropertiesObject =  Utilities.getEnvPropertiesobject(envPropertiesFilePath,sk)
 
    var sqlCon = Utilities.getConnection(sqlPropertyObject)
    val auditTbl = sqlPropertyObject.getMySqlDBName() + "." + sqlPropertyObject.getMySqlAuditTbl()

    var auditObj: com.hpe.config.AuditLoadObject = new AuditLoadObject("", "", "", "", "", "", "", 0, 0, 0, "", "", "", "", "")
    import scala.collection.JavaConversions._
    //val tblMappingArr = propertiesObject.getTableNameMapping().split(",")
    val idocColumnName = propertiesObject.getFilterExpression()

    /* logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Mapping Array: " + tblMappingArr.foreach { println })
    val tblTopicMap: Map[String, String] = new HashMap[String, String]()
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (element <- tblMappingArr) {
      logger.info("****************************" + element)
      val topic = element.split("\\|")(0)
      val histTbl = element.split("\\|")(1)
      tblTopicMap.put(topic, histTbl)
    }*/

    val src_sys_ky = propertiesObject.getMasterDataFields().split(",", -1)(0)
    val fl_nm = propertiesObject.getMasterDataFields().split(",", -1)(1)
    val ld_jb_nr = propertiesObject.getMasterDataFields().split(",", -1)(2)
    val jb_nm = propertiesObject.getMasterDataFields().split(",", -1)(3)
    val config_obj_file_nm = propertiesObject.getMasterDataFields().split(",", -1)(4)
    val auditBatchId = ld_jb_nr + "_" + Utilities.getCurrentTimestamp("yyyyMMddHHmmss")

    auditObj.setAudBatchId(auditBatchId)
    auditObj.setAudApplicationName("job_EA_loadNonConfigJSON")
    auditObj.setAudObjectName(propertiesObject.getObjName())
    var startTime = Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS")
    auditObj.setAudJobStartTimeStamp(startTime)
    auditObj.setAudLoadTimeStamp("9999-12-31 00:00:00")
    auditObj.setAudDataLayerName("fl_hist")
    auditObj.setAudSrcRowCount(0)
    auditObj.setAudTgtRowCount(0)
    auditObj.setAudErrorRecords(0)
    auditObj.setAudCreatedBy(configObject.getSpark().sparkContext.sparkUser)
    auditObj.setFlNm("")
    auditObj.setSysBtchNr(ld_jb_nr)
    try {

      val tblMappingArr = propertiesObject.getTableNameMapping().split(",")
      logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Mapping Array: " + tblMappingArr.foreach {
        println
      })

      val tblTopicMap: Map[String, String] = new HashMap[String, String]()
      for (element <- tblMappingArr) {
        val topic = element.split("\\|")(0)
        val histTbl = element.split("\\|")(1)
        tblTopicMap.put(topic, histTbl)
      }
      val topicList = propertiesObject.topicList.split(",").toList

      for (topic <- topicList) {

        val histTbl = propertiesObject.getDbName() + "." + tblTopicMap(topic)

        val spark = configObject.getSpark()
        val errTblNm = propertiesObject.getDbName() + "." + propertiesObject.getTgtTblErr()
        val rwTblNm = propertiesObject.getDbName() + "." + propertiesObject.getTgtTblRw()
        val refTblNm = propertiesObject.getDbName() + "." + propertiesObject.getTgtTblRef()

        val dataDFRef = spark.sqlContext.sql(f"""select * from $refTblNm limit 0""")
        val colListRef = dataDFRef.columns
        val dataDF = spark.sqlContext.sql(f"""select * from $errTblNm limit 0""")
        val colList = dataDF.columns
        val dataDFRaw = spark.sqlContext.sql(f"""select * from $rwTblNm limit 0""")
        val colListRaw = dataDFRaw.columns

        val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val obj_nm = propertiesObject.getObjName();

        var schema = StructType(propertiesObject.getColListSep().split(propertiesObject.getRcdDelimiter()).map(fieldName => StructField(fieldName, StringType, true)))
        val offset_schema = StructType(Array(StructField("intgtn_fbrc_msg_id", StringType, true)))
        schema = StructType(schema ++ offset_schema)

        val jsonToFlatUDF = udf(Utilities.jsonToString _)
        val putNullToFlatUDF = udf(Utilities.nullPutUDF _)
        val dqvalidate = udf(DataQuality.DQValidchck _)

        val dateFormatQuery = Utilities.prepareDateDoubleFormatQuery(colListRaw, propertiesObject.getDateCastFields(), propertiesObject.getDoublechkCol)

        val json_hive_raw_map_rdd = spark.sparkContext.parallelize(Seq(propertiesObject.getHiveJsonRawMap()))
        val jsonHeaderList: String = Utilities.getJsonHeaders(json_hive_raw_map_rdd, propertiesObject.getRcdDelimiter())
        var rawSqlQuery = Utilities.final_hive_json_mapper(json_hive_raw_map_rdd)
        rawSqlQuery = rawSqlQuery.replaceAll("FROM Temp_DF", ",intgtn_fbrc_msg_id as intgtn_fbrc_msg_id,'' AS src_sys_upd_ts, '" + src_sys_ky + "' as src_sys_ky, '' AS lgcl_dlt_ind,  current_timestamp() AS ins_gmt_ts, '' AS upd_gmt_ts, '' AS src_sys_extrc_gmt_ts,'' AS src_sys_btch_nr, '" + fl_nm + "'  AS fl_nm, '" + auditBatchId + "' AS ld_jb_nr FROM Temp_DF")

        var tgt_count: Long = 0
        var src_count: Long = 0
        var err_count: Long = 0
        var dfFile = configObject.spark.read.format("text").option("header", "false").load(propertiesObject.getSourceFilePath() + "*.txt")
        dfFile = dfFile.withColumnRenamed("value", "payload").withColumn("intgtn_fbrc_msg_id", lit(999999)).withColumn("ins_gmt_ts", lit(Utilities.getCurrentTimestamp())).withColumn("ld_jb_nr", lit(auditBatchId))
        val fileToHistLoadStatus = Utilities.storeDataFrame(dfFile, "Append", "ORC", histTbl)
        if (fileToHistLoadStatus == false) {
          auditObj.setAudJobStatusCode("failed")
          auditObj.setAudLoadTimeStamp(startTime)
          auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
          auditObj.setAudSrcRowCount(dfFile.count)
          auditObj.setAudTgtRowCount(auditObj.getAudSrcRowCount())
          //auditObj.setAudErrorRecords(err_count)
          auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
          //Check if SQL connection is active
          sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
          Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
          sqlCon.close()
          System.exit(1)
        } else {
            auditObj.setAudJobStatusCode("success")
            auditObj.setAudLoadTimeStamp(startTime)
            auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
            auditObj.setAudSrcRowCount(dfFile.count)
            auditObj.setAudTgtRowCount(auditObj.getAudSrcRowCount())
            auditObj.setAudErrorRecords(err_count)
            auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
            //Check if SQL connection is active
            sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
            Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        }

        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        var histLastMaxLoadTs: String = Utilities.readHistMaxLoadTimestamp(sqlCon, propertiesObject.getObjName(),auditTbl);

        auditObj.setAudDataLayerName("hist_rw")
        val histDf = configObject.getSpark().sql(f"""select payload,cast($offsetColumnName  as string) as intgtn_fbrc_msg_id,ins_gmt_ts from $histTbl where ins_gmt_ts > '$histLastMaxLoadTs'""")
        // logger.info("History Table Count=" + histDf.count())  //Removed to achieve speed --By EAP Support team.

        //If Data frame count is non-zero(0) Load from History to Ref table
        //Else load from Raw to Ref
        if (histDf.count() != 0) {
          var input_df_single_col = histDf.select("payload", "intgtn_fbrc_msg_id").dropDuplicates("payload")
          logger.info("########+++++++============>REGISTERING UDF===================#####")
          logger.info("########+++++++============>propertiesObject.getColListSep()===================#####" + propertiesObject.getColListSep())
          val newDF = input_df_single_col.withColumn("jsonFlatCol", jsonToFlatUDF(input_df_single_col("payload"), lit(propertiesObject.getRcdDelimiter()), lit(jsonHeaderList)))
          newDF.createOrReplaceTempView("temp_table_hist")
          //logger.debug(newDF.select("jsonFlatCol").show(false))
          var final_df = spark.sqlContext.sql(f"""select intgtn_fbrc_msg_id,explode(split(jsonFlatCol, '\\n')) as updatedCol, regexp_replace(regexp_replace(split(jsonFlatCol,'\\n')[0],'[@:-]| ','_'),'[()]','') as header from temp_table_hist""")

          //logger.debug(final_df.select("updatedCol").show(false))

          final_df = final_df.filter(!lower(final_df.col("updatedCol")).contains(propertiesObject.getFilterExpression().toLowerCase()))
          //logger.info("***********************+++++++++++++++Parsed History Count++++++++++++++**********************" + final_df.count());
          final_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
          final_df.createOrReplaceTempView("testdf")
          final_df = spark.sqlContext.sql("select * from testdf where length(trim(updatedCol))<>0")
          //final_df.select("updatedCol").show(false)
          final_df = final_df.withColumn("dataWithNull", putNullToFlatUDF(final_df("updatedCol"), lit(propertiesObject.getRcdDelimiter()), final_df("header"), lit(propertiesObject.getColListSep())))
          final_df = final_df.drop("header", "updatedCol")
          final_df.coalesce(20)
          //final_df = final_df.repartition(configObject.sparkConf.get("spark.executor.instances").toInt)

          logger.info("Raw SQL===================" + rawSqlQuery)
          var rdd = final_df.select("dataWithNull", "intgtn_fbrc_msg_id").rdd.map { row: Row => (row.getString(0) + propertiesObject.getRcdDelimiter() + row.getString(1)).substring(if ((row.getString(0).head).equals('"')) 1 else 0, (row.getString(0) + propertiesObject.getRcdDelimiter() + row.getString(1)).length()).replaceAll("\"" + propertiesObject.getRcdDelimiter(), propertiesObject.getRcdDelimiter()).replaceAll(propertiesObject.getRcdDelimiter() + "\"", propertiesObject.getRcdDelimiter()).replaceAll("\"" + propertiesObject.getRcdDelimiter() + "\"", propertiesObject.getRcdDelimiter()).replaceAll("\\\\\"", "\"").replaceAll("\n", System.lineSeparator()) }.map(line => line.split(propertiesObject.getRcdDelimiter(), -1)).map(line => Row.fromSeq(line))
          var loadingDF = spark.sqlContext.createDataFrame(rdd, schema)
          loadingDF.createOrReplaceTempView("Temp_DF")
          logger.info("Raw SQl::::::::::::::::" + rawSqlQuery)
          var rawDF_curr = spark.sqlContext.sql(rawSqlQuery)
          logger.info("#####################  ALL DONE #####################")
          var rawDfNullRemoved = rawDF_curr.na.fill("")

          //rawDfNullRemoved.select("*").show(false)
          // Currency Cast _______________________________________________________
          var currCastFields: String = propertiesObject.getCurrencyCastFields
          if (currCastFields != null && currCastFields.trim().length() != 0) {
            logger.info("==================== Currency Cast Changes Function ===========================================")
            var currCastFieldsArray: Array[String] = currCastFields.split(",")
            var noOfCols = currCastFieldsArray.length
            while (noOfCols > 0) {
              noOfCols = noOfCols - 1
              rawDfNullRemoved = Utilities.getcurrCastFields(rawDfNullRemoved, currCastFieldsArray(noOfCols))
            }
          }
          var result = rawDfNullRemoved.withColumn("newCol", concat_ws(propertiesObject.getRcdDelimiter(), rawDF_curr.schema.fieldNames.map(c => col(c)): _*))

          logger.info("=======propertiesObject.getBooleanchkCol()=================" + propertiesObject.getBooleanchkCol())
          
          result.persist(StorageLevel.MEMORY_AND_DISK_SER)

          var nf = result.withColumn("flag", dqvalidate(result("newCol"), lit(rawDF_curr.schema.fieldNames.toList.mkString(",")), lit(propertiesObject.getRcdDelimiter()), lit(propertiesObject.getNulchkCol()), lit(propertiesObject.getLnchkVal()), lit(propertiesObject.getDtfmtchkCol()), lit(propertiesObject.getIntchkCol()), lit(propertiesObject.getDoublechkCol()), lit(propertiesObject.getBooleanchkCol()), lit(propertiesObject.getLongchkCol)))
          nf = Utilities.nullifyEmptyStrings(nf)
          nf = nf.persist(StorageLevel.MEMORY_AND_DISK_SER)

          logger.info("==++++++++++++++++++++++++++++++++raw schema+++++++++++++++++++++++++++++==" + rawDF_curr.schema.fieldNames.toList.mkString(","))
          var validrawDF = nf.filter(nf("flag") === "VALID")
          validrawDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
          validrawDF.coalesce(20)
          var errorDF = nf.filter(nf("flag").contains("INVALID"))
          errorDF = errorDF.drop("newCol").withColumn("err_cd", lit("100")).withColumnRenamed("flag", "err_msg")
          errorDF.coalesce(20)
          logger.info("No of error table column===" + colList.length)
          src_count = input_df_single_col.count
          // var tgt_count = validrawDF.count
          logger.info("============LAST=================")
          var errorDFWithCol = errorDF.select(colList.head, colList.tail: _*)
          err_count = errorDFWithCol.count
          //logger.debug(errorDFWithCol.show(false))
          logger.info("================+++++++Date Format Query : RAW ++++++++++++++++++++++++++++===============")
          logger.info(dateFormatQuery)
          validrawDF.createOrReplaceTempView("Temp_DF")
          var validRawDfWithDateFormat = spark.sqlContext.sql(dateFormatQuery)
          validRawDfWithDateFormat.persist(StorageLevel.MEMORY_AND_DISK_SER)
          validRawDfWithDateFormat.coalesce(20)
          var histToRawLoadStatus = false
          var histToErrLoadStatus = false
          if (propertiesObject.getCustomSQL() != null && propertiesObject.getCustomSQL().size != 0) {
            validRawDfWithDateFormat.createOrReplaceTempView("temp_view")
            val customQuery = propertiesObject.getCustomSQL()
            val validRawDfAfterCustom = spark.sqlContext.sql(customQuery)
            tgt_count = validRawDfAfterCustom.count
            histToRawLoadStatus = Utilities.storeDataFrame(validRawDfAfterCustom, "Append", "ORC", propertiesObject.getDbName() + "." + propertiesObject.getTgtTblRw())
          } else {
            histToRawLoadStatus = Utilities.storeDataFrame(validRawDfWithDateFormat, "Append", "ORC", propertiesObject.getDbName() + "." + propertiesObject.getTgtTblRw())
            tgt_count = validRawDfWithDateFormat.count
          }
          histToErrLoadStatus = Utilities.storeDataFrame(errorDFWithCol, "Append", "ORC", errTblNm)

          if (histToRawLoadStatus == false && histToErrLoadStatus == false) {
            auditObj.setAudJobStatusCode("failed")
            auditObj.setAudLoadTimeStamp(startTime)
            auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
            auditObj.setAudSrcRowCount(src_count)
            auditObj.setAudTgtRowCount(tgt_count)
            auditObj.setAudErrorRecords(err_count)
            auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
            //Check if SQL connection is active
            sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
            Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
            sqlCon.close()
            System.exit(1)
          } else {
            auditObj.setAudJobStatusCode("success")
            auditObj.setAudLoadTimeStamp(startTime)
            auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
            auditObj.setAudSrcRowCount(src_count)
            auditObj.setAudTgtRowCount(tgt_count)
            auditObj.setAudErrorRecords(err_count)
            auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
            //Check if SQL connection is active
            sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
            Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
          }
           final_df.unpersist()
          nf.unpersist()
          result.unpersist()
          errorDFWithCol.unpersist()
        }
        startTime = Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS")
        auditObj.setAudJobStartTimeStamp(startTime)
        auditObj.setAudDataLayerName("rw_ref")
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        var lastRawMaxLoadTs: String = Utilities.readRawMaxLoadTimestamp(sqlCon, propertiesObject.getObjName(),auditTbl);
        var colListRefForQuery = colListRef.mkString(",")
        var refTableDf = configObject.getSpark().sql(f"""select $colListRefForQuery from $rwTblNm where ins_gmt_ts > '$lastRawMaxLoadTs'""")
        val refTblCnt = refTableDf.count()
        logger.info("Rw Table Cnt :" + refTblCnt)

        if (refTblCnt != 0) {

          val rawToRefLoadStatus = Utilities.storeDataFrame(refTableDf, "Append", "ORC", refTblNm)
          if (rawToRefLoadStatus == false) {
            auditObj.setAudJobStatusCode("failed")
            auditObj.setAudLoadTimeStamp(startTime)
            auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
            auditObj.setAudSrcRowCount(refTblCnt)
            auditObj.setAudTgtRowCount(refTblCnt)
            auditObj.setAudErrorRecords(0)
            auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
            //Check if SQL connection is active
            sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
            Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
            sqlCon.close()
            System.exit(1)
          } else {
            auditObj.setAudJobStatusCode("success")
            auditObj.setAudSrcRowCount(refTblCnt)
            auditObj.setAudTgtRowCount(refTblCnt)
            auditObj.setAudErrorRecords(0)
            auditObj.setAudLoadTimeStamp(startTime)
            auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
            auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
            //Check if SQL connection is active
            sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
            Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
          }
        }
       }
      sqlCon.close()
    } catch {
      case sslException: InterruptedException => {
        logger.error("Interrupted Exception")
        auditObj.setAudJobStatusCode("failed")
        auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
        auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
        //Check if SQL connection is active
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        sqlCon.close()
        System.exit(1)
      }
      case nseException: NoSuchElementException => {
        logger.error("No Such element found: " + nseException.printStackTrace())
        auditObj.setAudJobStatusCode("failed")
        auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
        auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
        //Check if SQL connection is active
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        sqlCon.close()
        System.exit(1)
      }
      case anaException: AnalysisException => {
        logger.error("SQL Analysis Exception: " + anaException.printStackTrace())
        auditObj.setAudJobStatusCode("failed")
        auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
        auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
        //Check if SQL connection is active
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        sqlCon.close()
        System.exit(1)
      }
      case connException: ConnectException => {
        logger.error("Connection Exception: " + connException.printStackTrace())
        auditObj.setAudJobStatusCode("failed")
        auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
        auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
        //Check if SQL connection is active
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        sqlCon.close()
        System.exit(1)
      }
      case exception: Exception => {
        logger.error(exception.printStackTrace())
        auditObj.setAudJobStatusCode("failed")
        auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss"))
        auditObj.setAudJobDuration((((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString()).toInt / 1000).toString())
        //Check if SQL connection is active
        sqlCon = Utilities.checkAndRestartSQLConnection(sqlCon, propertiesFilePath)
        Utilities.insertIntoAudit(sqlCon, auditObj, auditTbl)
        sqlCon.close()
        System.exit(1)
      }
    }
  }

}
