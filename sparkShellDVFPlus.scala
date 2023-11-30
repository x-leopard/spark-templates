import org.bdf.custom.CustomInputFormat
import scala.util.matching.Regex
import spark.implicits._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._


/**
 * Processes an RDD of paired values and extracts table information.
 *
 * @param pairedRddInitial The initial RDD containing paired values (String, Any).
 * @return An RDD of strings representing table information.
 */
def getTableInfofromPairedRdd(pairedRdd: RDD[(LongWritable, Text)]): RDD[(String, Array[(String, String)])] = {
	// Extracts table information from a CREATE TABLE statement
	val createTableRegex = """CREATE\s+TABLE\s+([a-zA-Z0-9_\.]+)\s*\(([\s\S]*?)\);""".r
	pairedRdd.map(x => x._2.toString).flatMap(inputStr => createTableRegex.findAllMatchIn(inputStr)
			 .map(x => (x.group(1).split("\\.")(1), x.group(2).split(",")
			 .map(_.trim).map(x=>x.split(" ")).map(x=>(x(0),x(1))))))
}

/**
 * Maps PostgreSQL data types to Spark SQL data types.
 *
 * @param postgresType The PostgreSQL data type as a string.
 * @return Corresponding Spark SQL data type.
 */
def postgresToSparkType(postgresType: String): DataType = {
  // Use a match expression to handle different cases
  postgresType match {
    // Numeric types
    case "smallint" => ShortType
    case "integer" => IntegerType
    case "bigint" => LongType
    case "numeric" | "decimal" => IntegerType  // DoubleType needs more arguments 
    case "real" => FloatType
    case "double precision" => DoubleType
    case "serial" => IntegerType
    case "bigserial" => LongType
    // Character types
    case "char" | "character" => StringType
    case "varchar" | "character varying" => StringType
    case "text" => StringType
    // Binary types
    case "bytea" => BinaryType
    // Date and time types
    case "date" => DateType
    case "time" | "time without time zone" => TimestampType
    case "timetz" | "time with time zone" => TimestampType
    case "timestamp" | "timestamp without time zone" => TimestampType
    case "timestamptz" | "timestamp with time zone" => TimestampType
    case "interval" => CalendarIntervalType
    // Boolean type
    case "boolean" => BooleanType
    // Array type
    case s if s.startsWith("_") => ArrayType(StringType)
    // Other types
    case s => StringType// Return the same type as a string
  }
}

/**
 * Writes data from an RDD to Hive tables based on specified rules.
 *
 * @param pairedRdd The initial RDD containing paired values (LongWritable, Text).
 * @param tableColumnsInfoRdd RDD containing table information (table name and column definitions).
 * @param tableRegex Regular expression to identify relevant data.
 * @param dataBaseWrite Name of the target database for writing.
 */
  def writerRddtoHive(pairedRdd: RDD[(LongWritable, Text)], tableColumnsInfoRdd: RDD[(String, Array[(String, String)])], tableRegex: Regex, dataBaseWrite: String): Unit = {
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
    println(s"processing Postgres national table $nationalTable")
    // Define a regular expression pattern to match lines containing the current table name.
    val tableNamePattern = ("^" + tableRegex.toString.replace("(\\w+)", nationalTable)).r
    // Filter the `rdd` variable to keep only the lines matching the pattern.
    val tableRDD = rdd.filter(x => tableNamePattern.findFirstIn(x).isDefined)
    // Cache the resulting RDD.
    tableRDD.cache()
    // Create a table name by concatenating the string `"distrib_"` with the table name.
    val tableName = "distrib_" + nationalTable
    // Transform the RDD into an RDD of rows using the `transformLines` function.
    val dataRdd = tableRDD.flatMap(transformLines)
    // If the RDD of rows is not empty, save the DataFrame as a table in the database `$dataBaseWrite` with the name `tableName`.
    if (!dataRdd.isEmpty) {
      println(s"Saving into Hive table $dataBaseWrite.$tableName")
      try {
        // Extract the column names from the first element of the RDD.
        val columnsAndTypes = tableColumnsInfoRdd.filter(x => x._1==nationalTable).map(x => x._2).collect()(0)
        // Create a schema for the data using the column names.
        val dataSchemaGeneric = StructType(columnsAndTypes.map(x=>x._1).map(fieldName => StructField(fieldName, StringType, nullable = true)))
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
        dataDf.show(5,truncate=false)
        dataDf.write.mode("overwrite").saveAsTable(s"$dataBaseWrite.$tableName")
      } 
      catch {
        case e: Exception => println(s"Error saving into table $dataBaseWrite.$tableName: ${e.getMessage}")
      }
    }
 }
}


// Main function

val appName = "CustomHadoopReader"
//val dataBase = "dv_dvf_work_layer"
val dataBase = "default"
val SqlFileInitial = "/tmp/dvf3/avril/dvf_initial.sql"
val SqlFilesMain = "/tmp/dvf3/avril/dvf_departements.sql"

// Create a SparkSession object named `spark` with the application name `appName`.
//val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
//val sc = spark.sparkContext
// Set a configuration property to allow creating managed tables using non-empty locations.
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
// Read initial sql file.
val pairedRddInitial = sc.newAPIHadoopFile(SqlFileInitial, classOf[CustomInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
// Extract the column names aand their types for each table in the CREATE TABLE statements from the initial sql file.
val tableColumnsInfoRdd =  getTableInfofromPairedRdd(pairedRddInitial)
// Define a regular expression pattern to match lines containing table names from initial dump sql file.
val copyTableInitialRegex = """\S*\.(\w+)\s+""".r
// Write data parsed from initial sql file to Hive tables in database `dataBase`
println("Writing data from initial sql file (DVF+) into Hive tables")
writerRddtoHive(pairedRddInitial, tableColumnsInfoRdd, copyTableInitialRegex, dataBase)   
// Read main sql file(s).
val pairedRddMAin = sc.newAPIHadoopFile(SqlFilesMain, classOf[CustomInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
// Define a regular expression pattern to match lines containing table names from main dump sql file(s).
val copyTableMainRegex = """\w+_\w+.(\w+)\s+""".r
// Write data parsed from main sql file(s) to Hive tables in database `dataBase`
println("Writing data from main sql file(s) (DVF+) into Hive tables")
writerRddtoHive(pairedRddMAin, tableColumnsInfoRdd, copyTableMainRegex, dataBase)

spark.stop()


  