from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, unix_timestamp, when, bround, avg,min,max,sum, udf
from pyspark.sql.types import TimestampType, DecimalType, StringType

if __name__ == '__main__':   

    # ==========================
    # 1.- Starting spark session
    # ==========================

    #spark = SparkSession.builder.master("local[1]").appName('Examples').config('spark.jars.packages','org.apache.spark:spark-avro_2.12:3.1.2').getOrCreate()
    spark = SparkSession.builder.appName('Examples').getOrCreate()

    # ===========================
    # 2.- Working with file types
    # ===========================

    #df = spark.read.format("json").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.json")
    #df = spark.read.format("csv").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.csv")
    #df = spark.read.format("parquet").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.parquet")
    #df = spark.read.format("avro").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/zipcodes.avro")

    # =====================
    # 3.- Loading main file
    # =====================

    df = spark.read.format("csv").option("header","true").load("hdfs://0.0.0.0:19000/enrique/data/Ejemplos/ExportCSV.csv")

    # ====================================
    # 4.- Working with missing or bad data
    # ====================================

    #df.select([count(when(isnan(c) | col(c).isnull(), c)).alias(c) for c in df.columns]).show()
    df = df.na.drop()
    #df.select([count(when(isnan(c) | col(c).isnull(), c)).alias(c) for c in df.columns]).show()

    # ======================================================
    # 5.- Working with structured transformations - select()
    # ======================================================

    #df.select(df["State"],df["Purchase (USD)"]).show()
    #df.select(col("State"),col("Purchase (USD)")).show() 

    # ==========================================================
    # 6.- Working with structured transformations - withColumn()
    # ==========================================================

    df = df.withColumn("Date",unix_timestamp(df["Date"],"M/d/yyyy h:mm a").cast(TimestampType()))
    df = df.withColumn("Purchase (USD)",df["Purchase (USD)"].cast(DecimalType(38,4)))
    #df.select(df["Date"]).show()

    # =================================================================
    # 7.- Working with structured transformations - withColumnRenamed()
    # =================================================================

    df = df.withColumnRenamed("SSN","Social Security Number")
    #df.printSchema()

    # ==========================================================
    # 8.- Working with structured transformations - selectExpr()
    # ==========================================================

    df = df.selectExpr("*", "case when (MONTH(Date)<=3) then concat(YEAR(Date) - 1,'Q3') when (MONTH(Date)<=6) then concat(YEAR(Date) - 1,'Q4') when (MONTH(Date)<=9) then concat(YEAR(Date) - 0,'Q1') ELSE concat(YEAR(Date) - 0,'Q2') end AS Quarter")
    #df.select(df["Quarter"]).show()

    # ==============================================================
    # 9.- Working with structured transformations - filter()/where()
    # ==============================================================

    #df.filter((df["State"] == "New York") & (df["Quarter"] == "2020Q1")).select(df["State"],df["Quarter"]).show()
    #df.where((df["State"] == "New York") & (df["Quarter"] == "2020Q1")).select(df["State"],df["Quarter"]).show()

    # ==========================================================================
    # 10.- Working with structured transformations - distinct()/dropDuplicates()
    # ==========================================================================

    #print("Distinct count: "+str(df.distinct().count()))
    #print("Drop duplicates count: "+str(df.dropDuplicates(["State","Retail Department"]).count()))

    # ===============================================================
    # 11.- Working with structured transformations - sort()/orderBy()
    # ===============================================================

    #df.select(df["Date"],df["Purchase (USD)"],df["State"]).sort(df["Date"].desc()).show()
    #df.select(df["Date"],df["Purchase (USD)"],df["State"]).orderBy(df["Date"].asc()).show()

    # ========================================================
    # 12.- Working with structured transformations - groupBy()
    # ========================================================

    #df.groupBy("Retail Department").agg(sum(bround(df["Purchase (USD)"], 2)).alias("Revenue (USD)")).show()
    #df.groupBy("State").agg(min(bround(df["Purchase (USD)"], 2)).alias("Min Purchase (USD)"),bround(avg(df["Purchase (USD)"]),2).alias("Avg Purchase (USD)"),max(bround(df["Purchase (USD)"], 2)).alias("Max Purchase (USD)")).show()

    # ========================================
    # 13.- SQL in Spark SQL - Temporary tables
    # ========================================

    #df.createOrReplaceTempView("shopping")
    #sqlDF = spark.sql("SELECT * FROM shopping")
    #spark.catalog.dropTempView("shopping")
    #sqlDF.show()

    # ===============================================
    # 14.- Working with User-Defined Functions (UDFs)
    # ===============================================    
    
    def removeDashChar(entry: str):
        """
        This function removes all ocurrences of a dash 
        character of an input string
        """
        return entry.replace("-", "")

    #df.select(df["Social Security Number"]).show()
    removeDashCharUDF = udf(lambda z: removeDashChar(z), StringType())
    df = df.withColumn("Social Security Number",removeDashCharUDF(col("Social Security Number")))
    #df.select(df["Social Security Number"]).show()

    # =======================
    # 15.- Working with Joins
    # =======================  

    new_df = df.limit(8)

    dept = [("Logistics",10), \
        ("Inventory",20), \
        ("Domestic operations",30), \
        ("International Operations",40), \
        ("Jewelry",50), \
        ("Cosmetics",60), \
    ]
    deptColumns = ["Department name","Department ID"]
    deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

    print("INNER JOIN")
    print("==========")
    new_df.join(deptDF,new_df["Retail Department"] ==  deptDF["Department name"],"inner") \
     .show()

    print("OUTER JOIN")
    print("==========")
    new_df.join(deptDF,new_df["Retail Department"] ==  deptDF["Department name"],"outer") \
     .show()

    print("LEFT JOIN")
    print("==========")
    new_df.join(deptDF,new_df["Retail Department"] ==  deptDF["Department name"],"left") \
     .show()

    print("RIGHT JOIN")
    print("==========")
    new_df.join(deptDF,new_df["Retail Department"] ==  deptDF["Department name"],"right") \
     .show()

    spark.stop()