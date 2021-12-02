import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, col, unix_timestamp, bround, min,avg,max, udf}
import org.apache.spark.sql.types.{TimestampType,  DecimalType}

object Example {
  def main(args: Array[String]): Unit = {   

    // ==========================
    // 1.- Starting spark session
    // ==========================     
    
    val spark = SparkSession.builder().appName("Examples").getOrCreate()

    // ===========================
    // 2.- Working with file types
    // ===========================  

    //val df = spark.read.format("json").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.json")
    //val df = spark.read.format("csv").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.csv")
    //val df = spark.read.format("avro").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.avro")
    //val df = spark.read.format("parquet").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.parquet")

    // =====================
    // 3.- Loading main file
    // =====================

    var df = spark.read.format("csv").option("header","true").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/ExportCSV.csv")
    
    // ====================================
    // 4.- Working with missing or bad data
    // ====================================

    //df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()
    df = df.na.drop()
    //df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()

    // ======================================================
    // 5.- Working with structured transformations - select()
    // ======================================================

     //df.select(df("State"),df("Purchase (USD)")).show
     //df.select(col("State"),col("Purchase (USD)")).show

    // ==========================================================
    // 6.- Working with structured transformations - withColumn()
    // ==========================================================

    df = df.withColumn("Date",unix_timestamp(df("Date"),"M/d/yyyy h:mm a").cast(TimestampType))
    df = df.withColumn("Purchase (USD)",df("Purchase (USD)").cast(DecimalType(38,4)))
    //df.select(df("Date")).show

    // =================================================================
    // 7.- Working with structured transformations - withColumnRenamed()
    // =================================================================

    df = df.withColumnRenamed("SSN","Social Security Number")
    //df.printSchema()

    // ==========================================================
    // 8.- Working with structured transformations - selectExpr()
    // ==========================================================

    df = df.selectExpr("*", "case when (MONTH(Date)<=3) then concat(YEAR(Date) - 1,'Q3') when (MONTH(Date)<=6) then concat(YEAR(Date) - 1,'Q4') when (MONTH(Date)<=9) then concat(YEAR(Date) - 0,'Q1') ELSE concat(YEAR(Date) - 0,'Q2') end AS Quarter")
    //df.select("Quarter").show

    // ==============================================================
    // 9.- Working with structured transformations - filter()/where()
    // ==============================================================

    //df.filter(df("State") === "New York" && df("Quarter") === "2020Q1").select(df("State"),df("Quarter")).show
    //df.where(df("State") === "New York" && df("Quarter") === "2020Q1").select(df("State"),df("Quarter")).show

    // ==========================================================================
    // 10.- Working with structured transformations - distinct()/dropDuplicates()
    // ==========================================================================

    //println("Distinct count: "+df.distinct.count())
    println("Drop duplicates count: "+df.dropDuplicates("State","Retail Department").count())

    // ===============================================================
    // 11.- Working with structured transformations - sort()/orderBy()
    // ===============================================================

    //df.select(df("Date"),df("Purchase (USD)"),df("State")).sort(df("Date").asc).show
    //df.select(df("Date"),df("Purchase (USD)"),df("State")).orderBy(df("Date").asc).show

    // ========================================================
    // 12.- Working with structured transformations - groupBy()
    // ========================================================

    //df.groupBy("Retail Department").agg(sum(bround(df("Purchase (USD)"), 2)).as("Revenue (USD)")).show
    //df.groupBy("State").agg(min(bround(df("Purchase (USD)"), 2)).as("Min Purchase (USD)"),bround(avg(df("Purchase (USD)")),2).as("Avg Purchase (USD)"),max(bround(df("Purchase (USD)"), 2)).as("Max Purchase (USD)")).show

    // ========================================
    // 13.- SQL in Spark SQL - Temporary tables
    // ========================================

    // df.createOrReplaceTempView("shopping")
    // val sqlDF = spark.sql("SELECT * FROM shopping")
    // spark.catalog.dropTempView("shopping")
    // sqlDF.show()

    // ===============================================
    // 14.- Working with User-Defined Functions (UDFs)
    // ===============================================

    val removeDashChar = (entry:String) => {
      /*
      This function removes all ocurrences of a dash 
      character of an input string
      */
      entry.replace("-", "")
    }
    df.select(df("Social Security Number")).show()
    val removeDashCharUDF = udf(removeDashChar)
    df = df.withColumn("Social Security Number",removeDashCharUDF(df("Social Security Number")))
    //df.select(df("Social Security Number")).show()

    df.createOrReplaceTempView("shopping")
    val sqlDF = spark.sql("DESCRIBE shopping")
    spark.catalog.dropTempView("shopping")
    sqlDF.show()

    // =======================
    // 15.- Working with Joins
    // =======================  

    val new_df = df.limit(8)

    val dept = Seq(("Logistics",10),
    ("Inventory",20),("Domestic operations",30),
    ("International Operations",40),("Jewelry",50),
    ("Cosmetics",60)
    )
       
    import spark.implicits._
    
    val deptColumns = Seq("Department name","Department ID")
    val deptDF = dept.toDF(deptColumns:_*)

    println("INNER JOIN")
    println("==========")
    new_df.join(deptDF,new_df("Retail Department") ===  deptDF("Department name"),"inner").show()

    println("OUTER JOIN")
    println("==========")
    new_df.join(deptDF,new_df("Retail Department") ===  deptDF("Department name"),"outer").show()

    println("LEFT JOIN")
    println("==========")
    new_df.join(deptDF,new_df("Retail Department") ===  deptDF("Department name"),"left").show()

    println("RIGHT JOIN")
    println("==========")
    new_df.join(deptDF,new_df("Retail Department") ===  deptDF("Department name"),"right").show()


    spark.stop()

  }  
}