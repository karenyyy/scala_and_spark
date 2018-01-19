```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

// customized schema, basically add (column names, column type)
val recordSchema = new StructType().add("sample", "long")
                                   .add("cThick", "integer")
                                   .add("uCSize", "integer")
                                   .add("uCShape", "integer")
                                   .add("mAdhes", "integer")
                                   .add("sECSize", "integer")
                                   .add("bNuc", "integer")
                                   .add("bChrom", "integer")
                                   .add("nNuc", "integer")
                                   .add("mitosis", "integer")
                                   .add("clas", "integer")
  
```


><pre>
> import org.apache.spark.sql.types._
> import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
> import org.apache.spark.sql.functions._
> recordSchema: org.apache.spark.sql.types.StructType = StructType(StructField(sample,LongType,true), StructField(cThick,IntegerType,true), StructField(uCSize,IntegerType,true), StructField(uCShape,IntegerType,true), StructField(mAdhes,IntegerType,true), StructField(sECSize,IntegerType,true), StructField(bNuc,IntegerType,true), StructField(bChrom,IntegerType,true), StructField(nNuc,IntegerType,true), StructField(mitosis,IntegerType,true), StructField(clas,IntegerType,true))





```scala
val spark = SparkSession.builder
      .appName("sql_sparksession")
      .getOrCreate
```


><pre>
> spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@13500dca





```scala
val df = spark.read.format("csv")
                   .option("header", false)
                   .schema(recordSchema)
                   .load("breast-cancer-wisconsin.data")
```


><pre>
> df: org.apache.spark.sql.DataFrame = [sample: bigint, cThick: int ... 9 more fields]





```scala
df.show()
```


><pre>
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> | sample|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|clas|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> |1000025|     5|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> |1002945|     5|     4|      4|     5|      7|  10|     3|   2|      1|   2|
> |1015425|     3|     1|      1|     1|      2|   2|     3|   1|      1|   2|
> |1016277|     6|     8|      8|     1|      3|   4|     3|   7|      1|   2|
> |1017023|     4|     1|      1|     3|      2|   1|     3|   1|      1|   2|
> |1017122|     8|    10|     10|     8|      7|  10|     9|   7|      1|   4|
> |1018099|     1|     1|      1|     1|      2|  10|     3|   1|      1|   2|
> |1018561|     2|     1|      2|     1|      2|   1|     3|   1|      1|   2|
> |1033078|     2|     1|      1|     1|      2|   1|     1|   1|      5|   2|
> |1033078|     4|     2|      1|     1|      2|   1|     2|   1|      1|   2|
> |1035283|     1|     1|      1|     1|      1|   1|     3|   1|      1|   2|
> |1036172|     2|     1|      1|     1|      2|   1|     2|   1|      1|   2|
> |1041801|     5|     3|      3|     3|      2|   3|     4|   4|      1|   4|
> |1043999|     1|     1|      1|     1|      2|   3|     3|   1|      1|   2|
> |1044572|     8|     7|      5|    10|      7|   9|     5|   5|      4|   4|
> |1047630|     7|     4|      6|     4|      6|   1|     4|   3|      1|   4|
> |1048672|     4|     1|      1|     1|      2|   1|     2|   1|      1|   2|
> |1049815|     4|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> |1050670|    10|     7|      7|     6|      4|  10|     4|   1|      2|   4|
> |1050718|     6|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> only showing top 20 rows





```scala
df.createOrReplaceTempView("cancerTable")
```




```scala
val sqlDF=spark.sql("select sample, bNuc from cancerTable")
sqlDF.show()
```


><pre>
> +-------+----+
> | sample|bNuc|
> +-------+----+
> |1000025|   1|
> |1002945|  10|
> |1015425|   2|
> |1016277|   4|
> |1017023|   1|
> |1017122|  10|
> |1018099|  10|
> |1018561|   1|
> |1033078|   1|
> |1033078|   1|
> |1035283|   1|
> |1036172|   1|
> |1041801|   3|
> |1043999|   3|
> |1044572|   9|
> |1047630|   1|
> |1048672|   1|
> |1049815|   1|
> |1050670|  10|
> |1050718|   1|
> +-------+----+
> only showing top 20 rows
> 
> sqlDF: org.apache.spark.sql.DataFrame = [sample: bigint, bNuc: int]





```scala
// create a case class
case class CancerClass(sample: Long, cThick: Int, uCSize: Int, uCShape: Int, mAdhes: Int, sECSize: Int, bNuc: Int, bChrom: Int, nNuc: Int, mitosis: Int, clas: Int)

```


><pre>
> defined class CancerClass





```scala
sc
```


><pre>
> res54: org.apache.spark.SparkContext = org.apache.spark.SparkContext@72561518





```scala
val cancerRDD=sc.textFile("breast-cancer-wisconsin.data")

val cancerDS=cancerRDD.map(_.split("."))
        .map(x=>CancerClass(x(0).trim.toLong,
                            x(1).trim.toInt, 
                            x(2).trim.toInt,
                            x(3).trim.toInt,
                            x(4).trim.toInt,
                            x(5).trim.toInt,
                            x(6).trim.toInt,
                            x(7).trim.toInt,
                            x(8).trim.toInt,
                            x(9).trim.toInt,
                            x(10).trim.toInt))
        .toDS()
```


><pre>
> cancerRDD: org.apache.spark.rdd.RDD[String] = breast-cancer-wisconsin.data MapPartitionsRDD[33] at textFile at <console>:89
> cancerDS: org.apache.spark.sql.Dataset[CancerClass] = [sample: bigint, cThick: int ... 9 more fields]





```scala
def binarize(s:Int):Int= s match {
 case 2=>0
 case 4=>1
}
```


><pre>
> binarize: (s: Int)Int





```scala
// udf: UserDefinedFunction
spark.udf.register("udfValueToCategory", (arg: Int) => binarize(arg))
```


><pre>
> res58: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))





```scala
// convert clas to binary factor, 2->0, 4->1
val sqlUDF = spark.sql("SELECT *, udfValueToCategory(clas) from cancerTable")
```


><pre>
> sqlUDF: org.apache.spark.sql.DataFrame = [sample: bigint, cThick: int ... 10 more fields]





```scala
sqlUDF.show()
```


><pre>
> +-------+------+------+-------+------+-------+----+------+----+-------+----+----------------------------+
> | sample|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|clas|UDF:udfValueToCategory(clas)|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+----------------------------+
> |1000025|     5|     1|      1|     1|      2|   1|     3|   1|      1|   2|                           0|
> |1002945|     5|     4|      4|     5|      7|  10|     3|   2|      1|   2|                           0|
> |1015425|     3|     1|      1|     1|      2|   2|     3|   1|      1|   2|                           0|
> |1016277|     6|     8|      8|     1|      3|   4|     3|   7|      1|   2|                           0|
> |1017023|     4|     1|      1|     3|      2|   1|     3|   1|      1|   2|                           0|
> |1017122|     8|    10|     10|     8|      7|  10|     9|   7|      1|   4|                           1|
> |1018099|     1|     1|      1|     1|      2|  10|     3|   1|      1|   2|                           0|
> |1018561|     2|     1|      2|     1|      2|   1|     3|   1|      1|   2|                           0|
> |1033078|     2|     1|      1|     1|      2|   1|     1|   1|      5|   2|                           0|
> |1033078|     4|     2|      1|     1|      2|   1|     2|   1|      1|   2|                           0|
> |1035283|     1|     1|      1|     1|      1|   1|     3|   1|      1|   2|                           0|
> |1036172|     2|     1|      1|     1|      2|   1|     2|   1|      1|   2|                           0|
> |1041801|     5|     3|      3|     3|      2|   3|     4|   4|      1|   4|                           1|
> |1043999|     1|     1|      1|     1|      2|   3|     3|   1|      1|   2|                           0|
> |1044572|     8|     7|      5|    10|      7|   9|     5|   5|      4|   4|                           1|
> |1047630|     7|     4|      6|     4|      6|   1|     4|   3|      1|   4|                           1|
> |1048672|     4|     1|      1|     1|      2|   1|     2|   1|      1|   2|                           0|
> |1049815|     4|     1|      1|     1|      2|   1|     3|   1|      1|   2|                           0|
> |1050670|    10|     7|      7|     6|      4|  10|     4|   1|      2|   4|                           1|
> |1050718|     6|     1|      1|     1|      2|   1|     3|   1|      1|   2|                           0|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+----------------------------+
> only showing top 20 rows





```scala
sqlUDF.select("clas", "UDF:udfValueToCategory(clas)").show()
```


><pre>
> +----+----------------------------+
> |clas|UDF:udfValueToCategory(clas)|
> +----+----------------------------+
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   4|                           1|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   2|                           0|
> |   4|                           1|
> |   2|                           0|
> |   4|                           1|
> |   4|                           1|
> |   2|                           0|
> |   2|                           0|
> |   4|                           1|
> |   2|                           0|
> +----+----------------------------+
> only showing top 20 rows




## RDD

- Partitions
    - the number of RDD partitions defines the level of data fragmentation
    - tune the number of partitions
        - fewer partitions than active stages means the cluster could be under-utilized
        - excessive number of partitions could impact the performance due to __higher disk and network I/O__
            
- Operations
     - transformations: create a new Dataset from an existing one
     - actions: return a value or result of a computation
  
- Persist (cache)
     - by default, Spark persists RDDs in memory, but it can spill them to disk if sufficient RAM isn't available
     - The in-memory storage of persistent RDDs can be in the of deserialized or serialized Java objects.
        - The deserialized option is faster, while the serialized option is more memory-efficient (but slower). 


## RDD -> DataFrame

```scala
val cancerDF=cancerRDD.toDF()
```


><pre>
> cancerDF: org.apache.spark.sql.DataFrame = [value: string]





```scala
def row(line: List[String]): Row = { Row(line(0).toLong, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9).toInt, line(10).toInt) }
```


><pre>
> row: (line: List[String])org.apache.spark.sql.Row





```scala
val data = cancerRDD.map(_.split(",").to[List]).map(row)
```


><pre>
> data: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[43] at map at <console>:89





```scala
val cancerDF = spark.createDataFrame(data, recordSchema)
cancerDF.show()
```


><pre>
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> | sample|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|clas|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> |1000025|     5|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> |1002945|     5|     4|      4|     5|      7|  10|     3|   2|      1|   2|
> |1015425|     3|     1|      1|     1|      2|   2|     3|   1|      1|   2|
> |1016277|     6|     8|      8|     1|      3|   4|     3|   7|      1|   2|
> |1017023|     4|     1|      1|     3|      2|   1|     3|   1|      1|   2|
> |1017122|     8|    10|     10|     8|      7|  10|     9|   7|      1|   4|
> |1018099|     1|     1|      1|     1|      2|  10|     3|   1|      1|   2|
> |1018561|     2|     1|      2|     1|      2|   1|     3|   1|      1|   2|
> |1033078|     2|     1|      1|     1|      2|   1|     1|   1|      5|   2|
> |1033078|     4|     2|      1|     1|      2|   1|     2|   1|      1|   2|
> |1035283|     1|     1|      1|     1|      1|   1|     3|   1|      1|   2|
> |1036172|     2|     1|      1|     1|      2|   1|     2|   1|      1|   2|
> |1041801|     5|     3|      3|     3|      2|   3|     4|   4|      1|   4|
> |1043999|     1|     1|      1|     1|      2|   3|     3|   1|      1|   2|
> |1044572|     8|     7|      5|    10|      7|   9|     5|   5|      4|   4|
> |1047630|     7|     4|      6|     4|      6|   1|     4|   3|      1|   4|
> |1048672|     4|     1|      1|     1|      2|   1|     2|   1|      1|   2|
> |1049815|     4|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> |1050670|    10|     7|      7|     6|      4|  10|     4|   1|      2|   4|
> |1050718|     6|     1|      1|     1|      2|   1|     3|   1|      1|   2|
> +-------+------+------+-------+------+-------+----+------+----+-------+----+
> only showing top 20 rows
> 
> cancerDF: org.apache.spark.sql.DataFrame = [sample: bigint, cThick: int ... 9 more fields]





```scala
val cancerDS = cancerDF.as[CancerClass]
```


><pre>
> cancerDS: org.apache.spark.sql.Dataset[CancerClass] = [sample: bigint, cThick: int ... 9 more fields]




## DataFrames and Datasets

-  DataFrame is similar to a table in a relational database, a pandas dataframe

    - evaluated lazily
        - syntax errors for DataFrames are caught during the compile stage
        - analysis errors are detected only during runtime
    - constructed from a wide array of sources, such as structured data files, Hive tables, databases, or RDD
        - source data can be read from local filesystems, HDFS, Amazon S3, and RDBMSs.
 

```scala
case class RestClass(id: String, name: String, street: String, city: String, phone: String)
```


><pre>
> defined class RestClass





```scala
val rest1DS = sc.textFile("zagats.csv")
                .map(_.split(","))
                .map(x => RestClass(
                  x(0).trim, 
                  x(1).trim, 
                  x(2).trim, 
                  x(3).trim, 
                  x(4).trim))
                .toDS()
```


><pre>
> rest1DS: org.apache.spark.sql.Dataset[RestClass] = [id: string, name: string ... 3 more fields]





```scala
rest1DS.show()
```


><pre>
> +---+--------------------+--------------------+------------------+------------+
> | id|                name|              street|              city|       phone|
> +---+--------------------+--------------------+------------------+------------+
> | id|                name|                addr|              city|       phone|
> |  1|    'apple pan  the'|'10801 w. pico bl...|         'west la'|310-475-3585|
> |  2|       'asahi ramen'|'2027 sawtelle bl...|         'west la'|310-479-2231|
> |  3|        'baja fresh'|   '3345 kimber dr.'|'westlake village'|805-498-4049|
> |  4|    'belvedere  the'|'9882 little sant...|   'beverly hills'|310-788-2306|
> |  5|  'benita\'s frites'|'1433 third st. p...|    'santa monica'|310-458-2889|
> |  6|        'bernard\'s'|  '515 s. olive st.'|     'los angeles'|213-612-1580|
> |  7|         'bistro 45'| '45 s. mentor ave.'|          pasadena|818-795-2478|
> |  8|     'brent\'s deli'|'19565 parthenia ...|        northridge|818-886-5679|
> |  9|'brighton coffee ...| '9600 brighton way'|   'beverly hills'|310-276-7732|
> | 10|'bristol farms ma...|'1570 rosecrans a...|          pasadena|310-643-5229|
> | 11|          'bruno\'s'|'3838 centinela a...|       'mar vista'|310-397-5703|
> | 12|        'cafe \'50s'| '838 lincoln blvd.'|            venice|310-399-1955|
> | 13|        'cafe blanc'|'9777 little sant...|   'beverly hills'|310-888-0108|
> | 14|        'cassell\'s'| '3266 w. sixth st.'|                la|213-480-8668|
> | 15|      'chez melange'|          '1716 pch'|   'redondo beach'|310-540-1222|
> | 16|           diaghilev|'1020 n. san vice...|    'w. hollywood'|310-854-1111|
> | 17|    'don antonio\'s'|'1136 westwood bl...|          westwood|310-209-1422|
> | 18|           'duke\'s'| '8909 sunset blvd.'|    'w. hollywood'|310-652-3100|
> | 19|      'falafel king'| '1059 broxton ave.'|          westwood|310-208-4444|
> +---+--------------------+--------------------+------------------+------------+
> only showing top 20 rows





```scala
val rest2DS = sc.textFile("fodors.csv")
                .map(_.split(","))
                .map(x => RestClass(
                  x(0).trim, 
                  x(1).trim, 
                  x(2).trim, 
                  x(3).trim, 
                  x(4).trim))
                .toDS()
```


><pre>
> rest2DS: org.apache.spark.sql.Dataset[RestClass] = [id: string, name: string ... 3 more fields]





```scala
rest2DS.show()
```


><pre>
> +---+--------------------+--------------------+---------------+------------+
> | id|                name|              street|           city|       phone|
> +---+--------------------+--------------------+---------------+------------+
> | id|                name|                addr|           city|       phone|
> |534|'arnie morton\'s ...|'435 s. la cieneg...|  'los angeles'|310/246-1501|
> |535|'art\'s delicates...|'12224 ventura bl...|  'studio city'|818/762-1221|
> |536|     'hotel bel-air'|'701 stone canyon...|      'bel air'|310/472-1211|
> |537|        'cafe bizou'|'14016 ventura bl...| 'sherman oaks'|818/788-3536|
> |538|           campanile|'624 s. la brea a...|  'los angeles'|213/938-1447|
> |539|   'chinois on main'|     '2709 main st.'| 'santa monica'|310/392-9025|
> |540|              citrus| '6703 melrose ave.'|  'los angeles'|213/857-0034|
> |541|               fenix|'8358 sunset blvd...|      hollywood|213/848-6677|
> |542|             granita|'23725 w. malibu ...|         malibu|310/456-0488|
> |543|'grill on the alley'|   '9560 dayton way'|  'los angeles'|310/276-0615|
> |544|  'restaurant katsu'|'1972 n. hillhurs...|  'los angeles'|213/665-1891|
> |545|      'l\'orangerie'|'903 n. la cieneg...|  'los angeles'|310/652-9770|
> |546|     'le chardonnay'| '8284 melrose ave.'|  'los angeles'|213/655-8880|
> |547|    'locanda veneta'|           '3rd st.'|  'los angeles'|310/274-1893|
> |548|           matsuhisa|'129 n. la cieneg...|'beverly hills'|310/659-9639|
> |549|          'the palm'|'9001 santa monic...|  'los angeles'|310/550-8811|
> |550|              patina| '5955 melrose ave.'|  'los angeles'|213/467-1108|
> |551|'philippe\'s the ...|'1001 n. alameda ...|  'los angeles'|213/628-3781|
> |552|      'pinot bistro'|'12969 ventura bl...|  'los angeles'|818/990-0500|
> +---+--------------------+--------------------+---------------+------------+
> only showing top 20 rows




#### then define a UDF to clean up and transform phone numbers in the second Dataset to match the format in the first file:

```scala
def formatPhoneNo(s: String): String = s match {
  case s if s.contains("/") => s.replaceAll("/", "-").replaceAll("- ", "-").replaceAll("--", "-") 
  case _ => s 
} 
```


><pre>
> formatPhoneNo: (s: String)String





```scala
val udfStandardizePhoneNos = udf[String, String]( x => formatPhoneNo(x) ) 
```


><pre>
> udfStandardizePhoneNos: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))





```scala
val rest2DSM1 = rest2DS.withColumn("stdphone", udfStandardizePhoneNos(rest2DS.col("phone")))
```


><pre>
> rest2DSM1: org.apache.spark.sql.DataFrame = [id: string, name: string ... 4 more fields]





```scala
rest2DSM1.show()
```


><pre>
> +---+--------------------+--------------------+---------------+------------+------------+
> | id|                name|              street|           city|       phone|    stdphone|
> +---+--------------------+--------------------+---------------+------------+------------+
> | id|                name|                addr|           city|       phone|       phone|
> |534|'arnie morton\'s ...|'435 s. la cieneg...|  'los angeles'|310/246-1501|310-246-1501|
> |535|'art\'s delicates...|'12224 ventura bl...|  'studio city'|818/762-1221|818-762-1221|
> |536|     'hotel bel-air'|'701 stone canyon...|      'bel air'|310/472-1211|310-472-1211|
> |537|        'cafe bizou'|'14016 ventura bl...| 'sherman oaks'|818/788-3536|818-788-3536|
> |538|           campanile|'624 s. la brea a...|  'los angeles'|213/938-1447|213-938-1447|
> |539|   'chinois on main'|     '2709 main st.'| 'santa monica'|310/392-9025|310-392-9025|
> |540|              citrus| '6703 melrose ave.'|  'los angeles'|213/857-0034|213-857-0034|
> |541|               fenix|'8358 sunset blvd...|      hollywood|213/848-6677|213-848-6677|
> |542|             granita|'23725 w. malibu ...|         malibu|310/456-0488|310-456-0488|
> |543|'grill on the alley'|   '9560 dayton way'|  'los angeles'|310/276-0615|310-276-0615|
> |544|  'restaurant katsu'|'1972 n. hillhurs...|  'los angeles'|213/665-1891|213-665-1891|
> |545|      'l\'orangerie'|'903 n. la cieneg...|  'los angeles'|310/652-9770|310-652-9770|
> |546|     'le chardonnay'| '8284 melrose ave.'|  'los angeles'|213/655-8880|213-655-8880|
> |547|    'locanda veneta'|           '3rd st.'|  'los angeles'|310/274-1893|310-274-1893|
> |548|           matsuhisa|'129 n. la cieneg...|'beverly hills'|310/659-9639|310-659-9639|
> |549|          'the palm'|'9001 santa monic...|  'los angeles'|310/550-8811|310-550-8811|
> |550|              patina| '5955 melrose ave.'|  'los angeles'|213/467-1108|213-467-1108|
> |551|'philippe\'s the ...|'1001 n. alameda ...|  'los angeles'|213/628-3781|213-628-3781|
> |552|      'pinot bistro'|'12969 ventura bl...|  'los angeles'|818/990-0500|818-990-0500|
> +---+--------------------+--------------------+---------------+------------+------------+
> only showing top 20 rows





```scala
rest1DS.createOrReplaceTempView("rest1Table") 

rest2DSM1.createOrReplaceTempView("rest2Table")

```


><pre>
> <console>:105: error: not found: value rest1Table
>               rest1Table
>               ^




```scala
spark.sql("SELECT count(*) from rest1Table, rest2Table where rest1Table.phone = rest2Table.stdphone").show()
```


><pre>
> +--------+
> |count(1)|
> +--------+
> |     112|
> +--------+





```scala
val sqlDF = spark.sql("SELECT a.name, b.name, a.phone, b.stdphone from rest1Table a, rest2Table b where a.phone = b.stdphone")
```


><pre>
> sqlDF: org.apache.spark.sql.DataFrame = [name: string, name: string ... 2 more fields]





```scala
sqlDF.show()
```


><pre>
> +--------------------+--------------------+------------+------------+
> |                name|                name|       phone|    stdphone|
> +--------------------+--------------------+------------+------------+
> |    'buckhead diner'|    'buckhead diner'|404-262-3336|404-262-3336|
> |'lespinasse (new ...|          lespinasse|212-339-6719|212-339-6719|
> |'tavern on the gr...|'tavern on the gr...|212-873-3200|212-873-3200|
> | 'brasserie le coze'| 'brasserie le coze'|404-266-1440|404-266-1440|
> |         bacchanalia|         bacchanalia|404-365-0410|404-365-0410|
> |      'pinot bistro'|      'pinot bistro'|818-990-0500|818-990-0500|
> |'hedgerose height...|'hedgerose height...|404-233-7673|404-233-7673|
> |             'jo jo'|             'jo jo'|212-223-5656|212-223-5656|
> |      'rainbow room'|      'rainbow room'|212-632-5000|212-632-5000|
> |'ritz-carlton res...|'restaurant  ritz...|404-659-0400|404-659-0400|
> |'ritz-carlton caf...|'restaurant  ritz...|404-659-0400|404-659-0400|
> |             abruzzi|             abruzzi|404-261-8186|404-261-8186|
> |        'river cafe'|        'river cafe'|718-522-5200|718-522-5200|
> | 'cafe des artistes'| 'cafe des artistes'|212-877-3500|212-877-3500|
> |'bone\'s restaurant'|           'bone\'s'|404-237-2663|404-237-2663|
> |                name|                name|       phone|       phone|
> |           matsuhisa|           matsuhisa|310-659-9639|310-659-9639|
> |    'plumpjack cafe'|    'plumpjack cafe'|415-563-4755|415-563-4755|
> |             aquavit|             aquavit|212-307-7311|212-307-7311|
> |    'heera of india'|    'heera of india'|404-876-4408|404-876-4408|
> +--------------------+--------------------+------------+------------+
> only showing top 20 rows
