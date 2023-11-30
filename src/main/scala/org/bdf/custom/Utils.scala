import org.apache.spark.sql.types._

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