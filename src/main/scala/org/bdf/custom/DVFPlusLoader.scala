package org.bdf.custom
import scala.util.matching.Regex
import spark.implicits._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.Logger


object MainDVFPlusLoader {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val dataBase = args(1)
    val SqlFileInitial = args(2)
    val SqlFilesMain = args(3)

    // Create a SparkSession object named `spark` with the application name `appName`.
    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    // Set a configuration property to allow creating managed tables using non-empty locations.
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
    // Read initial sql file.
    val pairedRddInitial = sc.newAPIHadoopFile(SqlFileInitial, classOf[CustomInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
    // Extract the column names aand their types for each table in the CREATE TABLE statements from the initial sql file.
    val createTableRegex = """CREATE\s+TABLE\s+([a-zA-Z0-9_\.]+)\s*\(([\s\S]*?)\);""".r
    val tableColumnsRdd =  pairedRddInitial.map(x => x._2.toString).flatMap(inputStr => createTableRegex.findAllMatchIn(inputStr).map(x => (x.group(1).split("\\.")(1), x.group(2).split(",").map(_.trim).map(x=>x.split(" ")).map(x=>(x(0),x(1))))))
    // Define a regular expression pattern to match lines containing table names.
    val tableNameRegex = """\S*\.(\w+)\s+""".r
    // Write data parsed from initial sql file to Hive tables in database `dataBase`
    logger.info("Writing data from initial sql file (DVF+) into Hive tables")
    writerRddtoHive(pairedRddInitial, tableColumnsRdd, tableNameRegex)   
    // Read main sql file(s).
    val pairedRddMAin = sc.newAPIHadoopFile(SqlFilesMain, classOf[CustomInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
    // Write data parsed from main sql file(s) to Hive tables in database `dataBase`
    logger.info("Writing data from main sql file(s) (DVF+) into Hive tables")
    writerRddtoHive(pairedRddMAin, tableColumnsRdd, tableNameRegex)

    spark.stop()
  }

  def writerRddtoHive(pairedRdd: RDD[(LongWritable, Text)], tableColumnsRdd: RDD[(String, Array[(String, String)])], tableRegex: Regex): Unit = {
  // Filter the paired RDD to keep only the pairs whose second element contains the string `"COPY "`. 
  // The regex should be replaced by a smarter one
  val TextRdd = pairedRdd.filter(x => x._2.toString.contains("COPY ")).map(x => x._2.toString)
  // Remove duplicates from the resulting RDD.
  val TextRddDistinct= TextRdd.distinct()
  // Create an RDD by splitting each element of the resulting RDD by the string `"COPY "` and keeping the second part.
  val rdd = TextRddDistinct.map(x => x.split("COPY ")(1))
  // Define a function that takes a string as input and returns an array of rows.
  def transformLines(text: String) = {
    val data_lines = text.split("\\n").drop(1).dropRight(1)
    val data = data_lines.map(line => line.split("\\t")).map(r => Row.fromSeq(r))
    data
  }
  // Create a list of national tables by applying the regular expression to each element of the `rdd` variable.
  val nationalTables = rdd.flatMap(text => tableRegex.findFirstMatchIn(text).map(_.group(1))).collect().toSet.toList

  for (nationalTable <- nationalTables) {
    logger.info(s"processing Postgres national table $nationalTable")
    // Define a regular expression pattern to match lines containing the current table name.
    val tableNamePattern = ("^" + tableRegex.toString.replace("(\\w+)", nationalTable).replace("\\","\\\\")).r
    // Filter the `rdd` variable to keep only the lines matching the pattern.
    val tableRDD = rdd.filter(x => tableNamePattern.findFirstIn(x).isDefined)
    // Cache the resulting RDD.
    tableRDD.cache()
    // Create a table name by concatenating the string `"distrib_"` with the table name.
    val tableName = "distrib_" + nationalTable
    // Transform the RDD into an RDD of rows using the `transformLines` function.
    val dataRdd = tableRDD.flatMap(transformLines)
    // If the RDD of rows is not empty, save the DataFrame as a table in the database `dv_dvf_work_layer` with the name `tableName`.
    if (!dataRdd.isEmpty) {
      logger.info(s"Saving into Hive table $dataBase.$tableName")
      try {
        // Extract the column names from the first element of the RDD.
        val columnsAndTypes = tableColumnsRdd.filter(x => x._1==nationalTable).map(x => x._2).collect()(0)
        // Create a schema for the data using the column names.
        val dataSchemaGeneric = StructType(columns.map(x=>x._1).map(fieldName => StructField(fieldName, StringType, nullable = true)))
        var dataDf = spark.createDataFrame(dataRdd, dataSchemaGeneric)
        // Cast non string formatted columns
        for (columnAndType <- columnsAndTypes){
          val column = columnAndType._1
          val postgresType = columnAndType._2
          val sparkType = postgresToSparkType(postgresType)
          if (sparkType != StringType) {
			      dataDf = dataDf.withColumn(column, dataDf(column).cast(sparkType))
		      }		
      	}
        //dataDf.show(5,truncate=false)
        dataDf.write.mode("overwrite").saveAsTable(s"$dataBase.$tableName")
      } 
      catch {
        case e: Exception => logger.error(s"Error saving into table $dataBase.$tableName: ${e.getMessage}")
      }
    }
 }
}
}

