import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class Scd2Process(session: com.snowflake.snowpark.Session
                  , sourceTable: String
                  , targetTable: String)
{
  var keyColumns: Array[String] = Array[String]()
  var scdStartDateColumnName: String = ""
  var scdEndDateColumnName: String = ""
  var scdActiveFlagColumnName = ""
  var refreshType: String = ""
  var runControlDate: String = null
  var scdActiveEndDate: String = null

  var sourceColumnArray: Seq[String] = session.table(this.sourceTable).schema.names.sorted
  var targetColumnArray: Seq[String] = session.table(this.targetTable).schema.names.sorted

  def validate_params() : (String, Seq[Any]) = {
    var error_message: String = ""
    var targetColumnCompareArray: Seq[Any] = Seq[Any]()

    val targetScdColumnArray = Seq(this.scdStartDateColumnName, this.scdEndDateColumnName, this.scdActiveFlagColumnName)

    if (sourceColumnArray.isEmpty)
      error_message = error_message + " Cannot retrieve Column List for Source Table (" + this.sourceTable + ")."
    if (targetColumnArray.isEmpty)
      error_message = error_message + " Cannot retrieve Column List for Source Table (" + this.targetTable + ")."

    if (this.scdStartDateColumnName.isEmpty || this.scdStartDateColumnName.trim() == "")
      error_message = error_message + " Audit Column (this.scdStartDateColumnName) not passed for Target table (" + this.targetTable + ")."

    if (this.scdEndDateColumnName.isEmpty || this.scdEndDateColumnName.trim() == "")
      error_message = error_message + " Audit Column (scdStartDateColumnName) not passed for Target table (" + this.targetTable + ")."

    if (targetColumnArray.nonEmpty)
      if (! targetColumnArray.contains(this.scdStartDateColumnName))
        error_message = error_message + " Target Table (" + this.targetTable + ") has missing column " + this.scdStartDateColumnName + "."
    if (! targetColumnArray.contains(this.scdEndDateColumnName))
      error_message = error_message + " Target Table (" + this.targetTable + ") has missing column " + this.scdEndDateColumnName + "."

    if (sourceColumnArray.nonEmpty && targetColumnArray.nonEmpty){
      if(this.keyColumns.isEmpty || this.keyColumns.length == 0)
        error_message = error_message + " Key Column(s) ("+ this.keyColumns.mkString(" ") + ") required."
      else if (this.keyColumns.length > 5) {
        error_message = error_message + "Distinct Key Columns(s) (" + this.keyColumns.distinct.mkString(" ") + ") > 5 not supported. Contact Developer's group for expansion."
      }
      else {
        var cnt_target: Int = 0
        var cnt_source: Int = 0

        for (col <-  this.keyColumns)
          if (! targetColumnArray.contains(col))
            cnt_target += 1

        for (col <- this.keyColumns)
          if (! sourceColumnArray.contains(col))
            cnt_source += 1

        if (cnt_target != 0)
          error_message = error_message + " All key Column(s) (" + this.keyColumns.mkString(" ") + ") not found in Target table columns " + targetColumnArray.mkString(" ") + "."

        if (cnt_source != 0)
          error_message = error_message + " All key column(s) (" + this.keyColumns.mkString(" ") + ") not found in Source table columns " + sourceColumnArray.mkString(" ") + "."
      }
    }

    for ( col <- targetColumnArray ){
      if (! targetScdColumnArray.contains(col))
        targetColumnCompareArray = targetColumnCompareArray:+ col
    }

    if (! (sourceColumnArray == targetColumnCompareArray)){
      val set_dif_source = sourceColumnArray.diff(targetColumnCompareArray)
      val set_dif_target = targetColumnCompareArray.diff(sourceColumnArray)
      val set_dif = set_dif_source ++ set_dif_target

      error_message = error_message + " Column list mismatched between source (" + this.sourceTable + ") and target (" + this.targetTable + "). Found in one but not in other : " + set_dif.mkString(" ") +" . Source Column List: " + sourceColumnArray.mkString(" ") + " Target Column List: " + targetColumnArray.mkString(" ") + " ."
    }
    else if (error_message.isEmpty){
      val maxStartDateRow = session.table(this.targetTable).agg(max(col(this.scdStartDateColumnName))).first(1)(0)
      if (! maxStartDateRow.isNullAt(0) && maxStartDateRow.get(0).toString.substring(0,10) > this.runControlDate)
        error_message = error_message + " Run Control Date parameter (" + this.runControlDate + ") cannot be earlier than the latest Start Date (" + maxStartDateRow.getDate(0).toString() + ") on Target Table / Column (" + this.targetTable + "/ " + this.scdStartDateColumnName + ")."
      val maxEndDateRow = session.table(this.targetTable).agg(max(col(this.scdEndDateColumnName))).first(1)(0)

      //Add condition here : col(this.scdEndDateColumnName)===lit(maxEndDateRow(0))
      if (session.table(this.targetTable).filter(col(this.scdEndDateColumnName).is_null || col(this.scdEndDateColumnName)===lit(maxEndDateRow(0))).groupBy(this.keyColumns).count.filter(col("count") > 1).count() > 0)
        error_message = error_message + " Duplicates found based on Key Columns (" + this.keyColumns.mkString(",") + ") in target table " + this.targetTable
      if (session.table(this.sourceTable).groupBy(this.keyColumns).count.filter(col("count") > 1).count() > 0)
        error_message = error_message + " Duplicates found based on Key Columns (" + this.keyColumns.mkString(",") + ") in source table " + this.sourceTable

      println("maxStartDateRow : " + maxStartDateRow(0))
      println("maxEndDateRow : " + maxEndDateRow(0))

      if (! maxEndDateRow.isNullAt(0) && this.scdActiveEndDate != null && maxEndDateRow.get(0).toString.substring(0, 10) > this.scdActiveEndDate)
        error_message = error_message + " scdActiveEndDate parameter (" + this.scdActiveEndDate + ") cannot be earlier than latest End Date (" + maxEndDateRow.getDate(0).toString() + ") on Target Table / Column (" + this.targetTable + "/ " + this.scdEndDateColumnName + ")."
      //===============================================================================================================
    }
    return (error_message, targetColumnCompareArray)
  }

  def construct_merge_key_columns(key_columns: Array[String] ): Array[String] = {
    var merge_key_columns: Array[String] = Array[String]()

    var merge_key_columns_index_start = 0
    var merge_key_columns_index_end = 4

    val key_columns_len = key_columns.length

    if (key_columns_len > 0){
      var cnt = 0
      for ( cnt <- merge_key_columns_index_start to merge_key_columns_index_end) {
        if (cnt < key_columns_len)
          merge_key_columns = merge_key_columns :+ key_columns(cnt)
        else
          merge_key_columns = merge_key_columns :+ key_columns(0)
      }
    }
    return merge_key_columns
  }

  def run(keyColumns: Array[String]
          , scdStartDateColumnName: String
          , scdEndDateColumnName: String
          , scdActiveflagColumnName: String
          , refreshType: String
          , runControlDate: String
          , scdActiveEndDate: String): String = {
    var return_code: Int = -1
    var error_message: String = ""
    var commonColumnArray: Seq[Any] = Seq[Any]()

    try{
      this.keyColumns = keyColumns.map(c => c.toUpperCase()).distinct
      this.scdStartDateColumnName = scdStartDateColumnName.toUpperCase()
      this.scdEndDateColumnName = scdEndDateColumnName.toUpperCase()
      this.scdActiveFlagColumnName = scdActiveflagColumnName.toUpperCase()
      this.refreshType = refreshType.toLowerCase().trim()
      this.runControlDate = if (runControlDate == null || runControlDate.isEmpty) LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) else runControlDate
      this.scdActiveEndDate = scdActiveEndDate

      val (validation_error_message, validation_commonColumnArray) = validate_params()
      error_message = validation_error_message
      commonColumnArray = validation_commonColumnArray
      if (error_message.trim().isEmpty) {
        val commonColumnString = commonColumnArray.mkString(",")
        val targetColumnOrderArray = targetColumnArray

        var dfSource = session.sql("select * " + ", hash(" + commonColumnString + ") as HASH_ROW_VALUE from " + this.sourceTable)
        println("=====================================================================================================")
        println("dfSource Names :" + dfSource.schema.names)


        val tmp_scdActiveDate = if (this.scdActiveEndDate == null) this.scdActiveEndDate else "'"+this.scdActiveEndDate+"'"

        val dfTarget = session.sql("select * " + ", hash(" + commonColumnString + ") as HASH_ROW_VALUE from " + this.targetTable + " where (" + this.scdEndDateColumnName + " IS NULL OR coalesce(" + this.scdEndDateColumnName + ", '1900-01-01') = coalesce(" + tmp_scdActiveDate + ", '1901-01-01' ) )")
        println("dfTarget Names :" + dfTarget.schema.names)


        val dfTargetEmpty = dfTarget.filter(col("HASH_ROW_VALUE") === 0)


        val dfSourceUpdates = dfSource.join(dfTarget, this.keyColumns,
          "inner").filter((! dfSource("HASH_ROW_VALUE") === dfTarget("HASH_ROW_VALUE"))
          && (dfTarget(this.scdEndDateColumnName).is_null
          || (coalesce(dfTarget(this.scdEndDateColumnName), to_date(lit("1901-01-01"))) ===
          coalesce(to_date(lit(this.scdActiveEndDate)), to_date(lit("1901-01-01"))))
          )).select(dfSource("*"))

        println("dfSourceUpdates Names :" + dfSourceUpdates.schema.names)
//        println(dfSourceUpdates.count())
//        println("SourceUpdates ->")
//        dfSourceUpdates.show()

        val dfSourceInserts = dfSource.join(dfTarget, this.keyColumns, "anti")

        println("dfSourceInserts Names :" + dfSourceInserts.schema.names)
        //println(dfSourceInserts.count())

        val dfTargetUpdates = dfTarget.join(dfSource, this.keyColumns,
          "inner").filter((!dfSource("HASH_ROW_VALUE") === dfTarget("HASH_ROW_VALUE"))
          && (dfTarget(this.scdEndDateColumnName).is_null
          || (coalesce(dfTarget(this.scdEndDateColumnName), to_date(lit("1901-01-01"))) ===
          coalesce(to_date(lit(this.scdActiveEndDate)), to_date(lit("1901-01-01")))))
          && (! coalesce(dfTarget(this.scdStartDateColumnName), to_date(lit("1901-01-01"))) ===
          to_date(lit(this.runControlDate))
          )).select(dfTarget("*"))

        println("dfTargetUpdates Names :" + dfTargetUpdates.schema.names)
//        println(dfTargetUpdates.count())
//        println("TargetUpdates ->")
//        dfTargetUpdates.show()

        val dfTargetUpdatesUnion = dfTargetEmpty.union(dfTargetUpdates)
          .select(targetColumnOrderArray)
          .withColumn(this.scdEndDateColumnName, dateadd("day", lit(-1), to_date(lit(this.runControlDate))))
          .withColumn(this.scdActiveFlagColumnName, lit("N"))
          .select(targetColumnOrderArray)

        println("dfTargetUpdatesUnion Names :" + dfTargetUpdatesUnion.schema.names)
//        println(dfTargetUpdatesUnion.count())
//        println("TargetUpdatesUnion ->")
//        dfTargetUpdatesUnion.show()

        var dfTargetDeletes = dfTargetEmpty.select(targetColumnOrderArray)

        println("dfTargetDeletes Names :" + dfTargetDeletes.schema.names)
        //println(dfTargetDeletes.count())

        if (this.refreshType == "full")
          dfTargetDeletes = dfTarget.join(dfSource, this.keyColumns, "anti")
            .select(targetColumnOrderArray)
            .withColumn(this.scdEndDateColumnName, dateadd("day", lit(-1), to_date(lit(this.runControlDate))))
            .withColumn(this.scdActiveFlagColumnName, lit("N"))
            .select(targetColumnOrderArray)

        val dfSourceInsertsUpdates = dfSourceInserts.union(dfSourceUpdates)
          .withColumn(this.scdStartDateColumnName, to_date(lit(this.runControlDate)))
          .withColumn(this.scdEndDateColumnName, lit(this.scdActiveEndDate))
          .withColumn(this.scdActiveFlagColumnName, lit("Y"))
          .select(targetColumnOrderArray)
          .union(dfTargetUpdatesUnion)
          .union(dfTargetDeletes)

        var mergeInsertDict: collection.immutable.Map[String, com.snowflake.snowpark.Column] = collection.immutable.Map[String, com.snowflake.snowpark.Column]()

        println("dfSourceInsertsUpdates Names :" + dfSourceInsertsUpdates.schema.names)
//        println(dfSourceInsertsUpdates.count())
//        println("SourceInsertsUpdates ->")
//        dfSourceInsertsUpdates.show()
//        println("=====================================================================================================")

        for (c <- dfSourceInsertsUpdates.schema.names) {
          mergeInsertDict += c -> dfSourceInsertsUpdates(c)
        }
        println("mergeInsertDict : "+mergeInsertDict)
        val merge_key_columns: Array[String] = construct_merge_key_columns(this.keyColumns)

        val mergeUpdateDict = mergeInsertDict.-(this.scdStartDateColumnName).-(this.keyColumns.mkString(","))
        println("mergeUpdateDict : " + mergeUpdateDict)

        val target = session.table(this.targetTable)

        val merge_build_obj = target.merge(dfSourceInsertsUpdates, ((target(merge_key_columns(0)) ===
          dfSourceInsertsUpdates(merge_key_columns(0)))
          && (target(merge_key_columns(1)) === dfSourceInsertsUpdates(merge_key_columns(1)))
          && (target(merge_key_columns(2)) === dfSourceInsertsUpdates(merge_key_columns(2)))
          && (target(merge_key_columns(3)) === dfSourceInsertsUpdates(merge_key_columns(3)))
          && (target(merge_key_columns(4)) === dfSourceInsertsUpdates(merge_key_columns(4)))
          &&(coalesce(target(this.scdStartDateColumnName), to_date(lit("1901-01-01"))) ===
          coalesce(dfSourceInsertsUpdates(this.scdStartDateColumnName), to_date(lit("1901-01-01")))
          ))).whenMatched.update(mergeUpdateDict).whenNotMatched.insert(mergeInsertDict)

        val merge_execute_result = merge_build_obj.collect()

        return_code = 0

        error_message = error_message + merge_execute_result.toString + " Control Date: " + runControlDate
      }
    } catch {
      case e: Exception => error_message = error_message + " Execution Error: " + e
    }
    return "{" + "\"return_code\": " + return_code.toString + ", \"return_message\": \"" + error_message + "\" )"
  }
}