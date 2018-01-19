
object Cells {
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
    

  /* ... new cell ... */

  val spark = SparkSession.builder
        .appName("sql_sparksession")
        .getOrCreate

  /* ... new cell ... */

  val df = spark.read.format("csv")
                     .option("header", false)
                     .schema(recordSchema)
                     .load("breast-cancer-wisconsin.data")

  /* ... new cell ... */

  df.show()

  /* ... new cell ... */

  df.createOrReplaceTempView("cancerTable")

  /* ... new cell ... */

  val sqlDF=spark.sql("select sample, bNuc from cancerTable")
  sqlDF.show()

  /* ... new cell ... */

  // create a case class
  case class CancerClass(sample: Long, cThick: Int, uCSize: Int, uCShape: Int, mAdhes: Int, sECSize: Int, bNuc: Int, bChrom: Int, nNuc: Int, mitosis: Int, clas: Int)

  /* ... new cell ... */

  sc

  /* ... new cell ... */

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

  /* ... new cell ... */

  def binarize(s:Int):Int= s match {
   case 2=>0
   case 4=>1
  }

  /* ... new cell ... */

  // udf: UserDefinedFunction
  spark.udf.register("udfValueToCategory", (arg: Int) => binarize(arg))

  /* ... new cell ... */

  // convert clas to binary factor, 2->0, 4->1
  val sqlUDF = spark.sql("SELECT *, udfValueToCategory(clas) from cancerTable")

  /* ... new cell ... */

  sqlUDF.show()

  /* ... new cell ... */

  sqlUDF.select("clas", "UDF:udfValueToCategory(clas)").show()

  /* ... new cell ... */

  val cancerDF=cancerRDD.toDF()

  /* ... new cell ... */

  def row(line: List[String]): Row = { Row(line(0).toLong, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9).toInt, line(10).toInt) }

  /* ... new cell ... */

  val data = cancerRDD.map(_.split(",").to[List]).map(row)

  /* ... new cell ... */

  val cancerDF = spark.createDataFrame(data, recordSchema)
  cancerDF.show()

  /* ... new cell ... */

  val cancerDS = cancerDF.as[CancerClass]

  /* ... new cell ... */

  case class RestClass(id: String, name: String, street: String, city: String, phone: String)

  /* ... new cell ... */

  val rest1DS = sc.textFile("zagats.csv")
                  .map(_.split(","))
                  .map(x => RestClass(
                    x(0).trim, 
                    x(1).trim, 
                    x(2).trim, 
                    x(3).trim, 
                    x(4).trim))
                  .toDS()

  /* ... new cell ... */

  rest1DS.show()

  /* ... new cell ... */

  val rest2DS = sc.textFile("fodors.csv")
                  .map(_.split(","))
                  .map(x => RestClass(
                    x(0).trim, 
                    x(1).trim, 
                    x(2).trim, 
                    x(3).trim, 
                    x(4).trim))
                  .toDS()

  /* ... new cell ... */

  rest2DS.show()

  /* ... new cell ... */

  def formatPhoneNo(s: String): String = s match {
    case s if s.contains("/") => s.replaceAll("/", "-").replaceAll("- ", "-").replaceAll("--", "-") 
    case _ => s 
  } 

  /* ... new cell ... */

  val udfStandardizePhoneNos = udf[String, String]( x => formatPhoneNo(x) ) 

  /* ... new cell ... */

  val rest2DSM1 = rest2DS.withColumn("stdphone", udfStandardizePhoneNos(rest2DS.col("phone")))

  /* ... new cell ... */

  rest2DSM1.show()

  /* ... new cell ... */

  rest1DS.createOrReplaceTempView("rest1Table") 
  
  rest2DSM1.createOrReplaceTempView("rest2Table")

  /* ... new cell ... */

  spark.sql("SELECT count(*) from rest1Table, rest2Table where rest1Table.phone = rest2Table.stdphone").show()

  /* ... new cell ... */

  val sqlDF = spark.sql("SELECT a.name, b.name, a.phone, b.stdphone from rest1Table a, rest2Table b where a.phone = b.stdphone")

  /* ... new cell ... */

  sqlDF.show()
}
                  