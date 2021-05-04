import java.io.{FileNotFoundException, IOException}
import java.math.BigInteger
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{HashMap, Properties}

import com.github.opendevl.JFlat
import main.scala.com.hpe.ope.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, lit, udf, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable.{Map, _}
import scala.util.control.Breaks.break
import java.util.zip.CRC32

object Utilities {
  val log = Logger.getLogger(getClass.getName)
  val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  /*
    * removeDuplicates function removes the duplicate rows from a DataFrame.
    *
    *
    * @param DataFrame			: Input DataFrame
    * @return DataFrame    : DataFrame with unique rows
    */
  def removeDuplicates(inputDF: DataFrame): DataFrame = {
    log.info("Removing duplicate rows")
    val columnList = inputDF.drop("intgtn_fbrc_msg_id").drop("ins_gmt_ts").columns
    val de_dup_DF = inputDF.dropDuplicates(columnList)
    log.info("Count After Dropping the Duplicate Rows :" + de_dup_DF.count())
    return de_dup_DF
  }

  /**
    * @param str
    * @return
    */
  def getStreamingPropertiesobject(str: String): StreamingPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: StreamingPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/
        pObj = new StreamingPropertiesObject(
          properties.getProperty("objName"),
          properties.getProperty("brokersList"),
          properties.getProperty("trustStoreLocString"),
          properties.getProperty("trustPwd"),
          properties.getProperty("keyStoreLocation"),
          properties.getProperty("keyStorePwd"),
          properties.getProperty("keyPwd"),
          properties.getProperty("autoCommit"),
          properties.getProperty("dbName"),
          properties.getProperty("jsonRoot"),
          properties.getProperty("topicList"),
          properties.getProperty("tableNameMapping"),
          properties.getProperty("jsonHiveMapTbl"),
          properties.getProperty("tgt_data_tbl"),
          properties.getProperty("tgt_ctrl_tbl"),
          properties.getProperty("colNm"),
          properties.getProperty("msgdelimeter"),
          properties.getProperty("NulchkCol"),
          properties.getProperty("lnchkVal"),
          properties.getProperty("DtfmtchkCol"),
          properties.getProperty("intchkCol"),
          properties.getProperty("doublechkCol"),
          properties.getProperty("booleanchkCol"),
          properties.getProperty("rcdDelimiter"),
          properties.getProperty("maxRunTime"),
          properties.getProperty("colListSep"),
          properties.getProperty("filterExpression"),
          properties.getProperty("tgtTblRw"),
          properties.getProperty("tgtTblErr"),
          properties.getProperty("tgtTblRef"),
          properties.getProperty("tgtTblEnr"),
          properties.getProperty("unqKeyCols"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields"),
          properties.getProperty("hiveJsonRawMap"),
          properties.getProperty("hiveJsonCtrlMap"),
          properties.getProperty("lookUpTable"),
          properties.getProperty("customSQL"),
          properties.getProperty("dateCastFields"),
          properties.getProperty("currencyConvFlag"),
          properties.getProperty("ptnrIdentificationFlag"),
          properties.getProperty("consumerGroupVal")
        )

      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }

  }

def getOpePtnrCmpJobPropertiesObject(str: String): PtnrCmpJobPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: PtnrCmpJobPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new PtnrCmpJobPropertiesObject(
          properties.getProperty("obj_nm"),
          properties.getProperty("job_nm"),
          properties.getProperty("dta_lyr_nm"),
          properties.getProperty("delimiter"),
          properties.getProperty("otpt_fl_nm"),
          properties.getProperty("otpt_file_extn"),
          properties.getProperty("otpt_file_path"),
          properties.getProperty("archive_file_path"),
          properties.getProperty("ld_qery"),
          properties.getProperty("ld_cndn"),
          properties.getProperty("ld_typ"),
          properties.getProperty("col_names"),
          properties.getProperty("src_data_layer"),
          properties.getProperty("tgt_data_layer"),
          properties.getProperty("ld_status"),
          properties.getProperty("ld_ord"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }
	
	
  def getDORPropertiesObject(str: String): DORPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: DORPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new DORPropertiesObject(
          properties.getProperty("objDorFullName"),
          properties.getProperty("objDbName"),
          properties.getProperty("objSrcTbl"),
          properties.getProperty("objTrgtTbl"),
          properties.getProperty("objRptgTrgtTbl"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields"),
          properties.getProperty("objPoTbl"),
          properties.getProperty("objMtrMstTbl"),
          properties.getProperty("objOptgConcernTbl"),
          properties.getProperty("objPftCntTbl"),
          properties.getProperty("objEqpmntTbl"),
          properties.getProperty("objExchRatesDmnsnTbl")

        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }


  /**
    * @param str
    * @return
    */
  def getCurrConvPropertiesobject(str: String): CurrencyConversionPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: CurrencyConversionPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new CurrencyConversionPropertiesObject(
          properties.getProperty("exchngTableName"),
          properties.getProperty("objLocalCurrencyCol"),
          properties.getProperty("objTrxAmtColMapping"),
          properties.getProperty("exchngTypeList"),
          properties.getProperty("objKeyCol"))
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }

  }

  /**
    *
    * @return
    */
  /**
    * @param str
    * @return
    */
  def getOpePtnrCustTrsnDedupPropertiesObject(str: String): PtnrCustTrsnDedupPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: PtnrCustTrsnDedupPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new PtnrCustTrsnDedupPropertiesObject(
          properties.getProperty("objName"),
          properties.getProperty("objSrcTbl"),
          properties.getProperty("objGrpTblNm"),
          properties.getProperty("objRptgPtnrMstrTblNm"),
          properties.getProperty("objRwAddrTblNm"),
          properties.getProperty("bmtCustTblNm"),
          properties.getProperty("bmtPtnrTblNm"),
          properties.getProperty("bmtbrTypTblNm"),
          properties.getProperty("mdmXrfTblNm"),
          properties.getProperty("mdmPtnrXrfTblNm"),
          // properties.getProperty("lgcyToNewBrTypBmtTblNm"),
          properties.getProperty("pftCntrBmtTblNm"),
          properties.getProperty("dealXclsnBmtTblNm"),
          properties.getProperty("dealTblNm"),
          properties.getProperty("dealRslrTblNm"),
          properties.getProperty("dealPrcDscTblNm"),
          properties.getProperty("pftCntrHrchyTblNm"),
          properties.getProperty("prtyDmnsntbl"),
          properties.getProperty("rptEligiblityBmtTblNm"),
          properties.getProperty("objSrcOrdTbl"),
          properties.getProperty("objSrcShpTbl"),
          // properties.getProperty("objOrdCndItmTblNm"),
          properties.getProperty("bmtCmsnEvtTblNm"),
          properties.getProperty("objPaAgrTblNm"),
          properties.getProperty("objTrgtTbl"),
          properties.getProperty("objPsPrsRefTblNm"),
          properties.getProperty("mdmGrphyHrchyTblNm"),
          properties.getProperty("rcsReporterBmtTblNm"),
          properties.getProperty("sgmCdToCtryCdBmtTblNm"),
          properties.getProperty("mcExceptionsTridentBmtTblNm"),
          properties.getProperty("bmtVndrRbtTblNm"),
          //properties.getProperty("zsdSplLcodesTblNm"),
          properties.getProperty("iCostTblNm"),
          properties.getProperty("iCostDmnsnTblNm"),
          properties.getProperty("clndrRptTblNm"),
          //properties.getProperty("ordHddrDmnsnTblNm"),
          properties.getProperty("custPtnrIdnFlg"),
          properties.getProperty("trsnDedupFlg"),
          properties.getProperty("spclDealBMTRestmtFlg"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("mccCodes"),
          properties.getProperty("stlmntsDocTypes"),
          properties.getProperty("ItemCategoriesList"),
          properties.getProperty("masterDataFields")


        )




      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }


  /**
    * @param str
    * @return
    */
  def getOpeSlsCmpPtnrCmpPropertiesObject(str: String): SlsCompPtnrCompPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: SlsCompPtnrCompPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new SlsCompPtnrCompPropertiesObject(
          properties.getProperty("objNm"),
          properties.getProperty("slsCpsnMnlAdjTblNm"),
          properties.getProperty("prtyDmnsnRefTblNm"),
          properties.getProperty("prtyDmnsnDmnsnTblNm"),
          properties.getProperty("ZymeCmnTblNm"),
          properties.getProperty("SerpCmnTblNm"),
          properties.getProperty("scoHistoryTblNm"),
          properties.getProperty("scoIntrmdtTbl"),
          properties.getProperty("scoIntrmdtTbl2"),
          properties.getProperty("skuLstTblNm"),
          properties.getProperty("pftCntrHrchyTblNm"),
          properties.getProperty("coGrpAllcRefTblNm"),
          properties.getProperty("coGrpAllcDmnsnTblNm"),
          properties.getProperty("prodHrchyRefTblNm"),
          properties.getProperty("pdmMtrlMstrGrpDmnsnTblNm"),
          properties.getProperty("optyFactTblNm"),
          properties.getProperty("optyRefTblNm"),
          properties.getProperty("sprtRqstRefTblNm"),
          properties.getProperty("sprtRqstDmnsnTblNm"),
          properties.getProperty("usrDmnsnTblNm"),
          properties.getProperty("opePsRefTblNm"),
          properties.getProperty("psRefTblNm"),
          properties.getProperty("bsnRshpGrpAsngmtDmnsnTblNm"),
          properties.getProperty("bsnRshpDmnsnTblNm"),
          properties.getProperty("bsnRshpXtndPflItmAsngmtDmnsnTblNm"),
          properties.getProperty("xtndPflBsnArGrpAsscnDmnsnTblNm"),
          properties.getProperty("psPrsRefTblNm"),
          properties.getProperty("dealDmnsnTblNm"),
          properties.getProperty("clndrRptTblNm"),
          properties.getProperty("pftCntrBmtRefTblNm"),
          properties.getProperty("pdmMtrlClssAllctnGrpDmnsnTblNm"),
          properties.getProperty("pdmClssTypChrcVlAsngmtDmnsnTblNm"),
          properties.getProperty("grphyHrchyFlattenedRef"),
          properties.getProperty("grphyHrchyFlattenedDmnsn"),
          properties.getProperty("opeSlsOrdSpmtRefTblNm"),
          properties.getProperty("slsSpmtRefTblNm"),
          properties.getProperty("slsSpmtDmnsnTblNm"),
          properties.getProperty("slsOrdRefTblNm"),
          properties.getProperty("slsOrdDmnsnTblNm"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }

  def getPtnrCmpPropertiesObject(str: String): PtnrCompPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: PtnrCompPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new PtnrCompPropertiesObject(
          properties.getProperty("objNm"),
          properties.getProperty("mnlAdjTblNm"),
          properties.getProperty("prtyDmnsnRefTblNm"),
          properties.getProperty("prtyDmnsnDmnsnTblNm"),
          properties.getProperty("ZymeCmnTblNm"),
          properties.getProperty("SerpCmnTblNm"),
          properties.getProperty("pcoHistoryTblNm"),
          properties.getProperty("coGrpAllcRefTblNm"),
          properties.getProperty("geoHrchyFlattenedRefTblNm"),
          properties.getProperty("geoHrchyFlattenedDmnsnTblNm"),
          properties.getProperty("bsnRshpGrpAsngmtTblNm"),
          properties.getProperty("coGrpAllctnDmsnTblNm"),
          properties.getProperty("bsnRshpXtndPflItmAsngmtDmnsnTblNm"),
          properties.getProperty("bsnRshpDmnsnTblNm"),
          properties.getProperty("xtndPflBsnArGrpAsscnDmnsnTblNm"),
          properties.getProperty("clndrRptTblNm"),
          properties.getProperty("pcIntermedteTblRstmt"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }


  /**
    * @param str
    * @return
    */
  def getChannelInventoryPropertiesObject(str: String): ChannelInventoryPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: ChannelInventoryPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new ChannelInventoryPropertiesObject(
          properties.getProperty("objNm"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields"),
          properties.getProperty("objSrcChnlSpltRefTblNm"),
          properties.getProperty("objSrcInvFctTblNm"),
          properties.getProperty("objSrcPosFctTblNm"),
          properties.getProperty("objTgtCoeffTblNm"),
          properties.getProperty("objTgtSpltTblNm"),
          properties.getProperty("objSrcClndrTblNm"),
          properties.getProperty("objTgtBmtCoeffTblNm"),
          properties.getProperty("objTgtBmtSpltTblNm"),
          properties.getProperty("rptgPtnrMstrDmnsn")

        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }


  /**
    * @param str
    * @return
    */
  def getAgedInventoryPropertiesObject(str: String): AgedInventoryPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: AgedInventoryPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new AgedInventoryPropertiesObject(
          properties.getProperty("objNm"),
          properties.getProperty("invntrySingleFact"),
          properties.getProperty("sellinSingleFact"),
          properties.getProperty("shpmntFact"),
          properties.getProperty("bmtAgedInvRef"),
          properties.getProperty("rptgPtnrMstrDmnsn"),

          properties.getProperty("geoHrchyTblNm"),
          properties.getProperty("opeAgedInvntry"),
          properties.getProperty("cldrRpt"),
          properties.getProperty("calendarDate"),
          properties.getProperty("cldrWTD"),
          properties.getProperty("cldrWeekEndDt"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields"),
          properties.getProperty("invTrnsctnId"),
          properties.getProperty("invReporterId"),
          properties.getProperty("invProdOption"),
          properties.getProperty("invProductId"),
          properties.getProperty("invRprtngPrtyId"),
          properties.getProperty("invRprtrBRTypCd"),
          properties.getProperty("invRprtrCtryCd"),
          properties.getProperty("invBucketDscnCd"),
          properties.getProperty("invBucketNr"),
          properties.getProperty("invProd_SKU_Nr"),
          properties.getProperty("invBaseQty"),
          properties.getProperty("invOptionQty"),
          properties.getProperty("invlastReportedTotalQty"),
          properties.getProperty("invlastReportedOptionQty"),
          properties.getProperty("invTrnsDt"),
          properties.getProperty("lst_rptd_lst_prc_inv_lcy_ext_cd"),
          properties.getProperty("lst_rptd_lst_prc_inv_usd_ext_cd"),
          properties.getProperty("lst_rptd_lst_prc_inv_lcy_unt_cd"),
          properties.getProperty("lst_rptd_lst_prc_inv_usd_unt_cd"),
          properties.getProperty("lst_rptd_ndp_inv_lcy_ext_cd"),
          properties.getProperty("lst_rptd_ndp_inv_usd_ext_cd"),
          properties.getProperty("lst_rptd_asp_inv_lcy_ext_cd"),
          properties.getProperty("lst_rptd_asp_inv_usd_ext_cd"),
          properties.getProperty("lst_rptd_asp_inv_lcy_unt_cd"),
          properties.getProperty("lst_rptd_asp_inv_usd_unt_cd"),
          properties.getProperty("lst_prc_inv_lcy_unt_cd"),
          properties.getProperty("lst_prc_inv_usd_ext_cd"),
          properties.getProperty("lst_prc_inv_usd_unt_cd"),
          properties.getProperty("lst_prc_inv_lcy_ext_cd"),
          properties.getProperty("ndp_inv_lcy_ext_cd"),
          properties.getProperty("ndp_inv_usd_ext_cd"),
          properties.getProperty("avg_deal_net_inv_lcy_ext_cd"),
          properties.getProperty("avg_deal_net_inv_usd_ext_cd"),
          properties.getProperty("sellinTrnsctnId"),
          properties.getProperty("sellinReporterId"),

          properties.getProperty("sellinRprtrPrtyId"),
          properties.getProperty("sellinRprtrBRTypCd"),
          properties.getProperty("sellinTrnsDt"),
          properties.getProperty("sellinProdOption"),
          properties.getProperty("sellinProductId"),
          properties.getProperty("sellinProd_SKU_Nr"),
          properties.getProperty("sellinBaseQty"),
          properties.getProperty("sellinOptionQty"),
          properties.getProperty("sellinlastReportedTotalQty"),
          properties.getProperty("shpmntTrnsctnId"),
          properties.getProperty("shpmntReporterPrtyId"),
          properties.getProperty("shpmntReporterBrTypCd"),
          properties.getProperty("shpmntProd_SKU_Nr"),
          properties.getProperty("shpmntOPEDrpShp"),
          properties.getProperty("shpmntTrnsDt"),
          properties.getProperty("shpmntProdOption"),
          properties.getProperty("shpmntProductId"),
          properties.getProperty("shpmntBaseQty"),
          properties.getProperty("shpmntOptionQty"),
          properties.getProperty("shpmntlastReportedTotalQty"),
          properties.getProperty("shpmntlastReportedOptionQty"),
          properties.getProperty("shpmntlastReportedBaseQty"),
          properties.getProperty("bmtRprtngPrtnrNm"),
          properties.getProperty("bmtRprtngPrtyId"),
          properties.getProperty("bmtRprtrBRTypCd"),
          properties.getProperty("bmtRprtrTypCd"),
          properties.getProperty("bmtrptrAgedInvFlg"),
          properties.getProperty("geoHrchyRgnCd")

        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }

  def getEMDMLegacyMappingPropertiesObject(str: String): EMDMLegacyMappingPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: EMDMLegacyMappingPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)

    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/

        pObj = new EMDMLegacyMappingPropertiesObject(
          properties.getProperty("objDbName"),
          properties.getProperty("objTableName"),
          properties.getProperty("mdmDbName"),
          properties.getProperty("mdmTableName"),
          properties.getProperty("mdmLegacyIdCol"),
          properties.getProperty("mdmSourceSystemCol"),
          properties.getProperty("objLegacySystemCode"),
          properties.getProperty("objLegacyIdCol"),
          properties.getProperty("mdmPartyIdCol")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null
    }
  }


  // Alternate Calendar Config class
  def getAlternateCalendarPropertiesobject(str: String): AlternateCalendarPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: AlternateCalendarPropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/
        pObj = new AlternateCalendarPropertiesObject(
          properties.getProperty("objName"),
          properties.getProperty("dbName"),
          properties.getProperty("fsclTblName"),
          properties.getProperty("dtDayTblName"),
          properties.getProperty("altTblName"),
          properties.getProperty("enrColName"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }
  }


  // partner Identification


  def getFilePropertiesobject(str: String): FilePropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var pObj: FilePropertiesObject = null
    log.info("++++++++++++++++++++++++++" + str)
    try {
      //						  properties.load(new FileInputStream(str))
      //							val path = getClass.getResource(str)
      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        /*										val source = Source.fromURL(path)
                    properties = new Properties()
                    properties.load(source.bufferedReader())*/
        pObj = new FilePropertiesObject(
          properties.getProperty("objName"),
          properties.getProperty("dbName"),
          properties.getProperty("fileBasePath"),
          properties.getProperty("delimiter"),
          properties.getProperty("archiveFileBasePath"),
          properties.getProperty("rejectFileBasePath"),
          properties.getProperty("NulchkCol"),
          properties.getProperty("lnchkVal"),
          properties.getProperty("DtfmtchkCol"),
          properties.getProperty("intchkCol"),
          properties.getProperty("doublechkCol"),
          properties.getProperty("booleanchkCol"),
          properties.getProperty("rcdDelimiter"),
          properties.getProperty("tgtTblRw"),
          properties.getProperty("tgtTblErr"),
          properties.getProperty("tgtTblRef"),
          properties.getProperty("tgtTblEnr"),
          properties.getProperty("unqKeyCols"),
          properties.getProperty("sqlPropertyPath"),
          properties.getProperty("masterDataFields"),
          properties.getProperty("headerOptions"),
          properties.getProperty("enclosedBy"),
          properties.getProperty("refrencedColumnMap"),
          properties.getProperty("loadType"),
          properties.getProperty("customSQL"),
          properties.getProperty("rejFileForErrorRec"))
      }
      return pObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }

  }

  def getSKeyPropertiesobject(str: String): SKeyObject =
  {
    val propfileSystem: FileSystem = FileSystem.newInstance(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(str))
    try {
      var properties: Properties = new Properties();
      properties.load(propFileInputStream);
      var pObj: SKeyObject = null
      log.info("++++++++++++++++++++++++++" + str)

      log.info("########################################Utilities::::::::::::::" + str.toString())
      if (str != null) {
        pObj = new SKeyObject(
          properties.getProperty("sKey")
        )
      }
      return pObj
    } catch {
      case ex: FileNotFoundException =>
      {log.error("Please place the \"sKey\" file in "+str+" path"  )
        sys.exit(1)}
      case ex: IOException           => {sys.exit(1)}

    }finally{
      if(propFileInputStream!=null){
        propFileInputStream.close()
      }
      if(propfileSystem!=null){
        propfileSystem.close()
      }

    }

  }

  def getSQLPropertiesObject(path: String, sk:SKeyObject): SQLPropertiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(path))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var sqlObj: SQLPropertiesObject = null
    try {
      if (path != null) {
        sqlObj = new SQLPropertiesObject(
          properties.getProperty("mySqlHostName"),
          properties.getProperty("mySqlDBName"),
          properties.getProperty("mySqlUserName"),
          // properties.getProperty("mySqlPassword",  ),
          AES.decrypt(properties.getProperty("mySqlPassword"),sk.getSKey()),
          properties.getProperty("mySqlPort"),
          properties.getProperty("mySqlAuditTbl"))

     //   log.info("Loading mysql properties file"+AES.decrypt(properties.getProperty("mySqlPassword"),sk.getSKey()))
      }
      return sqlObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }
    finally{
      if(propFileInputStream!=null){
        propFileInputStream.close()
      }
      if(propfileSystem!=null){
        propfileSystem.close()
      }

    }

  }

  def getTransformationPropertiesObject(path: String): TransformationPropetiesObject = {
    val propfileSystem: FileSystem = FileSystem.get(new Configuration)
    val propFileInputStream = propfileSystem.open(new Path(path))
    var properties: Properties = new Properties();
    properties.load(propFileInputStream);
    //	      var properties: Properties = null
    var sqlObj: TransformationPropetiesObject = null
    try {
      if (path != null) {
        sqlObj = new TransformationPropetiesObject(
          properties.getProperty("lookUpTables"),
          properties.getProperty("customSQL1"),
          properties.getProperty("customSQL2"))
      }
      return sqlObj
    } catch {
      case ex: FileNotFoundException => return null
      case ex: IOException => return null

    }

  }

  def getKafkaparam(kafkaObject: KafkaPropetiesObject): Map[String, Object] = {
    var kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaObject.getPropertiesObject().getBrokersList(),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaObject.getConsumergroup(),
      "auto.offset.reset" -> kafkaObject.getOffsetResetVal(),
      "enable.auto.commit" -> (kafkaObject.getPropertiesObject().getAutoCommit().toBoolean: java.lang.Boolean),
      "security.protocol" -> "SSL",
      "ssl.truststore.location" -> kafkaObject.getPropertiesObject().getTrustStoreLoc(),
      "ssl.truststore.password" -> kafkaObject.getPropertiesObject().getTrustPwd(),
      "ssl.keystore.location" -> kafkaObject.getPropertiesObject().getKeyStoreLocation(),
      "ssl.keystore.password" -> kafkaObject.getPropertiesObject().getKeyStorePwd(),
      "ssl.key.password" -> kafkaObject.getPropertiesObject().getKeyPwd(),
      "fetch.message.max.bytes" -> "15000000",
      "max.partition.fetch.bytes" -> "15000000")

    return kafkaParams
  }

  def getConnection(sqlPropertiesObject: SQLPropertiesObject): Connection = {
    val host = sqlPropertiesObject.getMySqlHostName()
    val port = sqlPropertiesObject.getMySqlPort()
    val username = sqlPropertiesObject.getMySqlUserName()
    val password = sqlPropertiesObject.getMySqlPassword()
    val dbName = sqlPropertiesObject.getMySqlDBName()
    try {
      Class.forName("com.mysql.jdbc.Driver");
      var con: Connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbName, username, password);
      // con.
      return con
    } catch {
      case e: Exception => return null
    }
  }

  def insertIntoAudit(sqlcon: Connection, auditObj: AuditLoadObject, fulltblName: String) = {

    try {
      log.info("WRITING INTO AUDIT LOG -- ")
      val audBatchId = auditObj.getAudBatchId()
      val audApplicationName = auditObj.getAudApplicationName
      val audObjectName = auditObj.getAudObjectName
      val audLayerName = auditObj.getAudDataLayerName
      val audStatusCode = auditObj.getAudJobStatusCode
      val audJobEndTimestamp = auditObj.getAudJobEndTimestamp()
      val audLoadTimeStamp = auditObj.getAudLoadTimeStamp()
      val audSrcRowCount = auditObj.getAudSrcRowCount()
      val audTgtRowCount = auditObj.getAudTgtRowCount()
      val audErrorRecords = auditObj.getAudErrorRecords()
      val audCreatedBy = auditObj.getAudCreatedBy()
      val audJobStartTimestamp = auditObj.getAudJobStartTimeStamp()
      val jobDuration = auditObj.getAudJobDuration()
      val flNm = auditObj.getFlNm()
      val sysBtchNr = auditObj.getSysBtchNr()

      val auditSQL = "INSERT INTO " + fulltblName + " SELECT \"" + audBatchId + "\" as btch_id, \"" + audApplicationName + "\" as appl_nm,\"" + audObjectName + "\" as obj_nm , \"" + audLayerName + "\" as dta_lyr_nm, \"" + audStatusCode + "\" as jb_stts_cd, \"" + audJobEndTimestamp + "\" as jb_cmpltn_ts, \"" + audLoadTimeStamp + "\" as ld_ts, \"" + audSrcRowCount + "\" as src_rec_qty, \"" + audTgtRowCount + "\" as tgt_rec_qty, \"" + audErrorRecords + "\" as err_rec_qty, \"" + audCreatedBy + "\" as crtd_by_nm ,\"" + audJobStartTimestamp + "\" as jb_strt_ts,\"" + jobDuration + "\" as jb_durtn_tm_ss,\"" + flNm + "\" as fl_nm,\"" + sysBtchNr + "\" as sys_btch_nr"
      log.info("===================Print SQL :: " + auditSQL)
      val preparedStmt = sqlcon.prepareStatement(auditSQL);
      preparedStmt.execute();

      log.info("INSERTED INTO AUDIT LOG")
    } catch {
      case e: Exception => e.printStackTrace()

    }

  }

  def readHistMaxLoadTimestamp(sqlcon: Connection, objName: String, dataLyrName:String): String = {
    var max_ld_ts: String = null
    try {
      log.info("Reading from Audit table")

      val stmt: Statement = sqlcon.createStatement()
      val auditSQL = "select case when max(ld_ts) is NULL then '1900-01-01 00:00:00' else max(ld_ts) end as max_ld_ts from jb_aud_tbl where  lower(dta_lyr_nm)='"+dataLyrName+"' and lower(jb_stts_cd) = 'success' and ld_ts <> '9999-12-31 00:00:00' and tgt_rec_qty >0 and obj_nm='" + objName + "'"
      log.info("===================Print SQL :: " + auditSQL)
      //val preparedStmt = sqlcon.prepareStatement(auditSQL);
      val rs: ResultSet = stmt.executeQuery(auditSQL)
      //Extact result from ResultSet rs
      while (rs.next()) {
        log.info("max(ld_ts)=" + rs.getTimestamp("max_ld_ts"))
        max_ld_ts = rs.getTimestamp("max_ld_ts").toString()
      }
      // close ResultSet rs
      rs.close();
    } catch {
      case e: Exception => e.printStackTrace()

    }
    max_ld_ts
  }

  def validateNotNull(sqlContext: HiveContext, df: DataFrame, primary_key_col_list: List[String]): List[DataFrame] = {
    var primary_correct_col = ""
    var primary_incorrect_col = ""

    for (z <- primary_key_col_list) {
      primary_correct_col = primary_correct_col + "and length(trim(" + z + "))>0 and  trim(" + z + ")<>'(null)' and trim(" + z + ") not like '%?'"
      primary_incorrect_col = primary_incorrect_col + "OR " + z + " is null OR length(trim(" + z + "))=0  OR trim(" + z + ")='(null)' OR trim(" + z + ") like '%?'"
    }
    df.show
    df.registerTempTable("null_data")
    val valid_select_query = "select * from null_data where " + (primary_correct_col.drop(3))
    val invalid_select_query = "select * from null_data where " + (primary_incorrect_col.drop(2))

    log.info("valid_records_query:- " + valid_select_query)
    log.info("invalid_records_query:- " + invalid_select_query)

    val validDF = sqlContext.sql(valid_select_query)
    val invalidDF = sqlContext.sql(invalid_select_query)
    List(validDF, invalidDF)
  }

  def convertDateTime(dt: String, fmt: String): String = {
    try {
      val dateFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(fmt)
      val jodatime: DateTime = dateFormatGeneration.parseDateTime(dt);
      val str: String = ISOFormatGeneration.print(jodatime);
      str
    } catch {
      case xe: IllegalArgumentException => return null
      case xe: NullPointerException => return null
    }
  }

  /**
    * getCurrentTimestamp function returns the current timestamp in ISO format
    *
    * @return : current timestamp in ISO format
    */
  def getCurrentTimestamp(): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  def getCurrentTimestamp(format: String): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(format)
    //("yyyy-MM-dd HH:mm:ss.SSS");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  def getFormatedTimeStamp(inputTimeStamp:DateTime,format: String): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(format)
    ISOFormatGeneration.print(inputTimeStamp)
  }

  def addNullCurrConvCols(exchngType: String, finalDF2: DataFrame): DataFrame = {

    //  var exchngType = "Budget"
    var finalDF = finalDF2
    // if (!finalDF2.columns.contains(exchngType + "_Currency_Rate_Valid")) {
    finalDF = finalDF.withColumn(exchngType + "_Currency_Rate_Valid", lit(null: String).cast(StringType)).withColumn("OPE_" + exchngType + "_Rate", lit(null: String).cast(StringType)).withColumn("OPE_" + exchngType + "_Rate_Amount_USD", lit(null: String).cast(StringType))


    finalDF

  }

  /**
    * @param hiveContext
    * @param dbName
    * @param tgtTableName
    * @return
    */
  def getTableCount(hiveContext: HiveContext, dbName: String, tgtTableName: String): Long = {
    val tableName = dbName + "." + tgtTableName
    val df = hiveContext.table(tableName)
    return df.count()
  }

  /**
    * storeDataFrame function stores the data frame to input HIVE table
    *
    * @param transformedDF : DataFrame to be stored
    * @param saveMode      : Mode of saving (OverWrite,Append,Ignore,ErrofIfExists)
    * @param storageFormat : Storage format for the target table (ORC,Parquet etc)
    * @param targetTable   :	Target HIVE table name with Database name
    * @return Boolean			: flag to indicate if the action was successful or not
    */
  @throws(classOf[Exception])
  def storeDataFrame(transformedDF: DataFrame, saveMode: String, storageFormat: String, targetTable: String): Boolean = {
    //log.info("Inside utility function"+transformedDF.count())
    var loadStatus = false
    try {
      val tblCount = transformedDF.count
      log.info("Writing to HIVE TABLE : " + targetTable + " :: DataFrame Count :" + tblCount)
      if (tblCount > 0 && transformedDF != null && saveMode.trim().length() != 0 && storageFormat.trim().length() != 0 && targetTable.trim().length() != 0) {
        log.info("SAVE MODE::" + saveMode + " Table Name :: " + targetTable + " Format :: " + storageFormat)
        log.info("----- Memory now 4 "+Runtime.getRuntime.freeMemory)
        transformedDF.repartition(20).write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())
        log.info("----- Memory now 5 "+Runtime.getRuntime.freeMemory)
        return true
      } else {
        return false
      }

    } catch {
      case e: Exception => e.printStackTrace();
        log.error("ERROR Writing to HIVE TABLE : " + targetTable);
        return false;
    }
    return loadStatus
  }

  def storeDataFrame(transformedDF: DataFrame, saveMode: String, storageFormat: String, targetTable: String,targetTableCount:Long): Boolean = {
    //log.info("Inside utility function"+transformedDF.count())
    var loadStatus = false
    try {
      val tblCount = targetTableCount
      log.info("Writing to HIVE TABLE : " + targetTable + " :: DataFrame Count :" + tblCount)
      if (tblCount > 0 && transformedDF != null && saveMode.trim().length() != 0 && storageFormat.trim().length() != 0 && targetTable.trim().length() != 0) {
        log.info("SAVE MODE::" + saveMode + " Table Name :: " + targetTable + " Format :: " + storageFormat)
        log.info("----- Memory now 4 "+Runtime.getRuntime.freeMemory)
        //        transformedDF.repartition(20).write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())
        if (tblCount > 1000000)
          transformedDF.repartition(30).write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())
        else
          transformedDF.repartition(1).write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())



        log.info("----- Memory now 5 "+Runtime.getRuntime.freeMemory)
        return true
      } else {
        return false
      }

    } catch {
      case e: Exception => e.printStackTrace();
        log.error("ERROR Writing to HIVE TABLE : " + targetTable);
        return false;
    }
    return loadStatus
  }
  /**
    * @param rdd
    * @return
    */
  def hive_json_mapper(rdd: RDD[String]): String = {
    var sql = ""
    rdd.collect.foreach { x =>
      val cols = x.split(';').toList
      cols.foreach { y =>
        sql = sql + y.replace("|", " AS ") + ","
      }
    }
    sql = sql.dropRight(1)
    return sql
  }

  def getJsonHeaders(rdd: RDD[String], dlmit: String): String = {
    var stringHeader: String = ""
    rdd.collect.foreach { x =>
      val cols = x.split(';').toList
      cols.foreach { y =>
        stringHeader = stringHeader.trim() + y.split("\\|")(0).trim() + dlmit.trim()
      }
    }
    stringHeader = stringHeader.substring(0, stringHeader.lastIndexOf(dlmit))
    return stringHeader
  }

  def final_hive_json_mapper(rdd: RDD[String]): String = {
    var sql = ""
    rdd.collect.foreach { x =>
      val cols = x.split(';').toList
      cols.foreach { y =>
        sql = sql + y.replace("|", " AS ") + ","
      }
    }
    sql = "select " + sql.dropRight(1) + " FROM Temp_DF"
    return sql
  }

  def prepareDateFormatQuery(colList: Array[String], dtFmtCol: String): String = {
    var sql = ""
    var hm: Map[String, String] = scala.collection.mutable.Map[String, String]()
    if (dtFmtCol != null && dtFmtCol.size != 0) {
      val colArray = dtFmtCol.split(',').toList

      colArray.foreach { x =>
        //if(x.split('|').toList(1).trim().length()<=10){
        hm.put(x.split('|').toList(0).trim(), x.split('|').toList(1).trim())
        //}
      }
    }
    colList.foreach { x =>
      if (hm.contains(x)) {
        sql = sql + "TO_DATE(CAST(UNIX_TIMESTAMP(" + x + ", '" + hm.getOrElse(x, "yyyy-MM-dd HH:mm:ss") + "') AS TIMESTAMP))" + " AS " + x + ","
      } else {
        sql = sql + x + " AS " + x + ","
      }
    }
    sql = "select " + sql.dropRight(1) + " FROM Temp_DF"
    return sql
  }

  /**
    * @param sqlQuery
    * @param colMap
    * @return
    */
  def sqlGenerator(sqlQuery: String, colMap: Map[String, List[String]]): Map[String, String] = {
    var colWithSQL = scala.collection.mutable.Map[String, String]()

    for ((cols, listNull) <- colMap) {
      var sql = sqlQuery
      listNull.foreach { x =>
        sql = sql.replaceAllLiterally(x + " AS", "NULL AS")
      }
      colWithSQL(cols) = "select " + sql + " FROM Temp_DF"
    }
    return colWithSQL
  }

  /**
    * @param str
    * @return
    */
  def jsonToString(str: String, delimeter: String, header: String): String = {
    try {
      val flatMe = new JFlat(str);

      return flatMe.json2Sheet().headerSeparator("_").write2csv(delimeter)
    } catch {
      case ex: Exception =>
        //log.fatal("Exception while parsing JSON")
        log.fatal("Exception Parsing JSON:" + ex)
        //System.exit(1)
        var error = StringBuilder.newBuilder
        error.append("\n")
        val list: Array[String] = header.split(delimeter)
        var count: Int = 0
        for (fileds <- list) {
          if (count == 0) {
            //val subEx = ex.getCause.toString().replaceAll("\n", " ")
            error.append(" Exception while Parsing JSON : " + str.substring(0, 100) + ".... with exception :" + ex.getCause).append(delimeter)
          } else {
            error.append("N/A").append(delimeter)
          }
          count = count + 1
        }
        // error =
        return header + error.substring(0, error.lastIndexOf(delimeter))
      //val flatMe = new JFlat("{\"LastModifiedDate\":\"Exception Parsing JSON\"}");
      //return flatMe.json2Sheet().headerSeparator("_").write2csv(delimeter)

    }
  }

  /* *
    * @param str
    * @return
    */
  def generateHiveColumns(rdd: RDD[String]): List[String] = {
    var hiveColList = List[String]()
    rdd.collect.foreach { x =>
      val mapList = x.split(";").toList
      mapList.foreach { x =>
        hiveColList = x.split("\\|")(0).trim() :: hiveColList
      }
      hiveColList = hiveColList.filter(_ != "NULL")
    }
    return hiveColList
  }

  def generateHeaderMap(jsonHeaderDF: DataFrame, jsonColList: List[String], delimter: String): Map[String, List[String]] = {
    var headerMap = scala.collection.mutable.Map[String, List[String]]()

    jsonHeaderDF.rdd.map(_.getString(0)).collect().foreach { x =>
      log.info("==================================headerColumn======" + x)
      var p = x.split(delimter, -1).toList
      p = jsonColList.diff(p)
      headerMap(x) = p
    }
    return headerMap
  }

  def insert(list: List[String], i: Int, value: String): List[String] = {
    val (front, back) = list.splitAt(i)
    front ++ List(value) ++ back
  }

  /*def nullPutUDF(str: String, delimeter: String, header: String, cols: String): String = {
    import collection.breakOut
    var colList: List[String] = cols.split(delimeter).map(_.trim)(breakOut)
    var trimmedList: List[String] = str.split(delimeter).map(_.trim)(breakOut)
    var headerTrimmedList: List[String] = header.split(delimeter).map(_.trim)(breakOut)
    var i = 0;
    for (i <- 0 to colList.size - 1) {
      if (i > headerTrimmedList.size - 1) {
        trimmedList = Utilities.insert(trimmedList, i, null)
        headerTrimmedList = Utilities.insert(headerTrimmedList, i, null)
      } else if (!colList(i).equalsIgnoreCase(headerTrimmedList(i))) {
        trimmedList = Utilities.insert(trimmedList, i, null)
        headerTrimmedList = Utilities.insert(headerTrimmedList, i, null)
      }
    }
    return trimmedList.mkString(delimeter)
  }*/
  def CreateMap(data: List[String], header: List[String]): Map[String, String] = {
    import scala.collection.breakOut
    val hm: LinkedHashMap[String, String] = (header zip data) (breakOut)
    return hm
  }

  def nullPutUDF(str: String, delimeter: String, header: String, cols: String): String = {
    import collection.breakOut
    var colList: List[String] = cols.toUpperCase().split(delimeter).map(_.trim)(breakOut)
    var trimmedList: List[String] = str.split(delimeter, -1).map(_.trim)(breakOut)
    var headerTrimmedList: List[String] = header.toUpperCase().split(delimeter, -1).map(_.trim)(breakOut)
    var hm: Map[String, String] = CreateMap(trimmedList, headerTrimmedList)
    var fullMap: Map[String, String] = collection.mutable.LinkedHashMap[String, String]()
    /*for(i <- 0 until colList.size)
			{
				val newKey:String=colList(i).toUpperCase()
						val newVal:String=hm.getOrElse(newKey, "\"null\"")
						fullMap.put(newKey, newVal)
			}*/
    colList.foreach { x =>
      val newKey: String = x.toUpperCase()
      val newVal: String = hm.getOrElse(newKey, "")
      fullMap.put(newKey, newVal)
    }

    fullMap.valuesIterator.toList.mkString(delimeter)

  }

  def generateSeqID(naturalKey: String): Long = {
    var m: MessageDigest = null
    var seqId: Long = 0
    try {
      m = MessageDigest.getInstance("MD5")
      m.reset()
      m.update(naturalKey.getBytes, 0, naturalKey.getBytes.length)
      val digest: Array[Byte] = m.digest()
      val bigInt: BigInteger = new BigInteger(digest)
      val original: Long = bigInt.longValue()
      var signum: Int = java.lang.Long.signum(original)
      seqId = original
      while (signum == -1) {
        val m1: MessageDigest = MessageDigest.getInstance("MD5")
        m1.reset()
        m1.update(("" + seqId).getBytes, 0, ("" + seqId).getBytes.length)
        val digest1: Array[Byte] = m1.digest()
        val bigInt1: BigInteger = new BigInteger(digest1)
        seqId = bigInt1.longValue()
        signum = java.lang.Long.signum(seqId)
      }
    } catch {
      case e: NoSuchAlgorithmException => e.printStackTrace()
    }
    seqId
  }

  def nullifyEmptyStrings(df: DataFrame): DataFrame = {
    var in = df
    for (e <- df.columns) {
      in = in.withColumn(e, when(length(col(e)) === 0, lit(null: String)).otherwise(col(e)))
    }
    in
  }

  def performCDC(sqlContext: SQLContext, history_df: DataFrame, incremental_df: DataFrame, primary_key_col_list: Array[String]): DataFrame = {
    import sqlContext.implicits._
    log.info("Starting CDC")
    val dataSchema = history_df.columns
    //    val final_incremental_df = incremental_df.except(history_df)
    val dfInnerJoin = history_df.filter(col("dl_load_flag")
      .eqNullSafe("Y")).as("L1").join(broadcast(incremental_df), primary_key_col_list)
      .select($"L1.*").select(dataSchema.head, dataSchema.tail: _*)
    val unchangedData = history_df.except(dfInnerJoin)
    val changedData = dfInnerJoin.drop("dl_load_flag").withColumn("dl_load_flag", lit("N")).select(dataSchema.head, dataSchema.tail: _*)
    val finalData = unchangedData.unionAll(incremental_df.drop("dl_load_date").withColumn("dl_load_date", lit(getCurrentTimestamp)).select(dataSchema.head, dataSchema.tail: _*))
      .unionAll(changedData)
    log.info("Completed CDC !!!")
    finalData
  }

  /* *
    * validateNotNull function checks whether the primary key column has
    *  Null values and if any, loads them to the error table
    *
    * @param SQLContext
    * @param DataFrame which contains the input table data
    * @param List which contains the primary key columns read from the yaml file
    * @return DataFrame of valid and invalid records
    */

  def validateNotNull(sqlContext: SQLContext, df: DataFrame, primary_key_col_list: List[String]): List[DataFrame] = {
    var primary_correct_col = ""
    var primary_incorrect_col = ""

    for (z <- primary_key_col_list) {
      primary_correct_col = primary_correct_col + "and length(trim(" + z + "))>0 and  trim(" + z + ")<>'(null)' and trim(" + z + ") not like '%?'"
      primary_incorrect_col = primary_incorrect_col + "OR " + z + " is null OR length(trim(" + z + "))=0  OR trim(" + z + ")='(null)' OR trim(" + z + ") like '%?'"

    }
    df.registerTempTable("incoming_data")
    val valid_select_query = "select * from incoming_data where " + (primary_correct_col.drop(3))
    val invalid_select_query = "select * from incoming_data where " + (primary_incorrect_col.drop(2))

    log.info("valid_records_query:- " + valid_select_query)
    log.info("invalid_records_query:- " + invalid_select_query)

    val validDF = sqlContext.sql(valid_select_query)
    val invalidDF = sqlContext.sql(invalid_select_query)

    List(validDF, invalidDF)

  }

  /* *
    * ValidateReferentialIntegrity function checks if all the
    *  referential integrity constrains are satisfied and if not,
    *  loads those records to the error table
    *
    * @param SQLContext
    * @param HashMap
    * @param DataFrame contains the input table data
    * @param List contains the columns to be checked for referential integrity, read from the yaml file
    * @return DataFrame records which satisfy and don't satisfy the constraint
    */

  def validateReferentialIntegrity(sqlContext: SQLContext, m: HashMap[String, String], dfInput: DataFrame, colNames: List[String]): List[DataFrame] = {
    val colList = m.keySet().toArray().toList
    var trueList = dfInput
    var falseList = dfInput.limit(0)
    for (list <- colList) {
      val stringToSplit = m.get(list).toUpperCase()

      val finalArray = stringToSplit.split(",")
      val inListInter: (String => String) = (arg: String) => {
        if (finalArray.contains(arg.toUpperCase())) "true" else "false"
      }
      val sqlfunc = udf(inListInter)

      val interDF = dfInput.withColumn("Result", sqlfunc(col(list.toString())))
      interDF.registerTempTable("interDF_tbl")

      val resdf = sqlContext.sql("""select * from interDF_tbl where Result = "false"""")
      val falseListInter = resdf.drop("Result")
      falseList = falseList.unionAll(falseListInter)

    }
    if (!(colNames.isEmpty)) {
      falseList = falseList.dropDuplicates(colNames)
    }
    List(trueList, falseList)
  }

  /* *
    *  ValidateDateFormat function checks if the date fields
    *  in the data frame are in the format expected in yaml file
    *  and if not, load those records to the error table
    *
    * @param SparkContext
    * @param SQLContext
    * @param DataFrame contains the records which passed the referential integrity check
    * @param HashMap
    * @return DataFrame data set of all and error records
    */

  def historyCheck(sqlcon: Connection, auditObj: AuditLoadObject, fileName: String, fulltblName: String): Boolean = {
    log.info("Reading from Audit table")

    var status: Boolean = false
    try {
      //checking the file is already loaded to the table or not.
      val stmt: Statement = sqlcon.createStatement()

      val auditSQL = "select * from " + fulltblName + " where fl_nm=\"" + fileName + "\" and jb_stts_cd = \"success\""
      val rs: ResultSet = stmt.executeQuery(auditSQL)
      //Extact result from ResultSet rs
      if (rs.getRow > 0) {
        log.info("File has already been ingested")
        status = false
      } else {
        status = true
      }
      // close ResultSet rs
      rs.close();
    } catch {
      case e: Exception => e.printStackTrace()

    }
    return status
  }

  def archiveFile(fileLocation: String, archiveLocation: String, fs: FileSystem): Unit = {
    //log.info("hadoop fs -mv " + fileLocation + " " +"hdfs://EAHPEDEV"+archiveLocation)
    log.info(archiveLocation + fileLocation.toString().substring(fileLocation.toString().lastIndexOf("/") + 1))
    fs.rename(new Path(fileLocation), new Path(archiveLocation + fileLocation.toString().substring(fileLocation.toString().lastIndexOf("/") + 1)))
    //val processStatus: Process  = Process("hadoop fs -mv " + fileLocation + " " +"hdfs://EAHPEDEV"+archiveLocation).run()
    /*val processStatus: Process  = Process(s"""hdfs dfs -mv $fileLocation $archiveLocation""").run()
    log.info("archival status:" + processStatus)
    if (processStatus == 0)
      return true
    else
      return false
      *
      */
  }

  def truncateTable(dbName: String, tableName: String, sqlc: HiveContext): Boolean = {
    try {
      sqlc.sql(f"""insert overwrite table $dbName.$tableName select * from $dbName.$tableName limit 0 """)
      var noOfRecords = sqlc.sql(f"""select count(1) from $dbName.$tableName""")
      log.info("no of records in table :" + noOfRecords.count())
      if (noOfRecords.count() > 1)
        return false
      else
        return true
    } catch {
      case t:
        Throwable => log.error(tableName + " not found in the database" + dbName); return true

    }

  }

  def validateHeader(inputRDD: RDD[String], indicator: String): RDD[String] = {
    val headerrDD = inputRDD.first()
    val rawinputRDD = inputRDD.filter(row => row != headerrDD)
    return rawinputRDD
  }

  def checkForRfrentilaIntegrity(sqlcontext: SQLContext, bmtcol: ListBuffer[String], m: HashMap[String, String], inputDF: DataFrame): Boolean = {

    val refrencetable = m.keySet().toArray().toList
    val maptoBMTcol = bmtcol.map(x => col(x))
    val validationdf = inputDF.select(maptoBMTcol: _*)

    for (table <- refrencetable) {
      val stringToSplit = m.get(table).toUpperCase()
      val finalArray = stringToSplit.split(",").toArray.toList
      val refrenceCol = finalArray.map(c => col(c))
      val refrenceDF = sqlcontext.sql(f"""select * from $table""")
      val maptoCol = refrenceDF.select(refrenceCol: _*)

      if (validationdf.except(refrenceDF).count() > 0) {
        return false
        break()
      }
    }

    return true
  }

  def generateUniqueKey(strConcatedInput: String): String = {
    val crc=new CRC32
    crc.update(strConcatedInput.getBytes)
    crc.getValue.toString()
  }


}
