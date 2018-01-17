

```scala
// Data location
val airlinesPath="airlines.csv"
val airportsPath="airports.csv"
val flightsPath="flights.csv"
```


```scala
// Load one dataset 
val airlines=sc.textFile(airlinesPath)
```


```scala
airlines
```


```scala
// View the entire dataset
airlines.collect()
```


```scala
// Get the first line
airlines.first()
```




Code,Description




```scala
// View a few lines
airlines.take(10)
```




Array(Code,Description, "19031","Mackey International Inc.: MAC", "19032","Munz Northern Airlines Inc.: XY", "19033","Cochise Airlines Inc.: COC", "19034","Golden Gate Airlines Inc.: GSA", "19035","Aeromech Inc.: RZZ", "19036","Golden West Airlines Co.: GLW", "19037","Puerto Rico Intl Airlines: PRN", "19038","Air America Inc.: STZ", "19039","Swift Aire Lines Inc.: SWT")




```scala
airlines.count()
```




   1580




```scala
airlines.filter(x => !x.contains("Description"))
```




   MapPartitionsRDD[3] at filter at <console>:25




```scala
val airlinesWoHeader=airlines.filter(x => !x.contains("Description"))
```


```scala
airlinesWoHeader.take(10)
```




   Array("19031","Mackey International Inc.: MAC", "19032","Munz Northern Airlines Inc.: XY", "19033","Cochise Airlines Inc.: COC", "19034","Golden Gate Airlines Inc.: GSA", "19035","Aeromech Inc.: RZZ", "19036","Golden West Airlines Co.: GLW", "19037","Puerto Rico Intl Airlines: PRN", "19038","Air America Inc.: STZ", "19039","Swift Aire Lines Inc.: SWT", "19040","American Central Airlines: TSF")




```scala
val airlinesParsed=airlinesWoHeader.map(_.split(','))
```


```scala
airlinesParsed.take(10)
```




   Array(Array("19031", "Mackey International Inc.: MAC"), Array("19032", "Munz Northern Airlines Inc.: XY"), Array("19033", "Cochise Airlines Inc.: COC"), Array("19034", "Golden Gate Airlines Inc.: GSA"), Array("19035", "Aeromech Inc.: RZZ"), Array("19036", "Golden West Airlines Co.: GLW"), Array("19037", "Puerto Rico Intl Airlines: PRN"), Array("19038", "Air America Inc.: STZ"), Array("19039", "Swift Aire Lines Inc.: SWT"), Array("19040", "American Central Airlines: TSF"))




```scala
airlines.map(_.length).take(10)
```




   Array(16, 40, 41, 36, 40, 28, 39, 40, 31, 36)




```scala
def notHeader(row: String): Boolean = {
    !row.contains("Description")
    }
airlines.filter(notHeader).take(10)
```




  Array("19031","Mackey International Inc.: MAC", "19032","Munz Northern Airlines Inc.: XY", "19033","Cochise Airlines Inc.: COC", "19034","Golden Gate Airlines Inc.: GSA", "19035","Aeromech Inc.: RZZ", "19036","Golden West Airlines Co.: GLW", "19037","Puerto Rico Intl Airlines: PRN", "19038","Air America Inc.: STZ", "19039","Swift Aire Lines Inc.: SWT", "19040","American Central Airlines: TSF")




```scala
airlines.filter(notHeader).map(_.split(",")).take(10)
```




   Array(Array("19031", "Mackey International Inc.: MAC"), Array("19032", "Munz Northern Airlines Inc.: XY"), Array("19033", "Cochise Airlines Inc.: COC"), Array("19034", "Golden Gate Airlines Inc.: GSA"), Array("19035", "Aeromech Inc.: RZZ"), Array("19036", "Golden West Airlines Co.: GLW"), Array("19037", "Puerto Rico Intl Airlines: PRN"), Array("19038", "Air America Inc.: STZ"), Array("19039", "Swift Aire Lines Inc.: SWT"), Array("19040", "American Central Airlines: TSF"))




```scala
airlinesWoHeader.map(_.replace("\"","")).map(_.split(',')).map(x => (x(0).toInt,x(1))).take(10)
```




   Array((19031,Mackey International Inc.: MAC), (19032,Munz Northern Airlines Inc.: XY), (19033,Cochise Airlines Inc.: COC), (19034,Golden Gate Airlines Inc.: GSA), (19035,Aeromech Inc.: RZZ), (19036,Golden West Airlines Co.: GLW), (19037,Puerto Rico Intl Airlines: PRN), (19038,Air America Inc.: STZ), (19039,Swift Aire Lines Inc.: SWT), (19040,American Central Airlines: TSF))




```scala
def parseLookup(row: String): (String,String)={
 val x = row.replace("\"","").split(',')
 (x(0),x(1))
}
```


```scala
import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate

case class Flight(date: LocalDate,
                  airline: String ,
                  flightnum: String,
                  origin: String ,
                  dest: String ,
                  dep: LocalTime,
                  dep_delay: Double,
                  arv: LocalTime,
                  arv_delay: Double ,
                  airtime: Double ,
                  distance: Double
                   )


```


```scala




def parse(row: String): Flight={

  val fields = row.split(",")
  val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
  val timePattern = DateTimeFormat.forPattern("HHmm")

  val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
  val airline: String = fields(1)
  val flightnum: String = fields(2)
  val origin: String = fields(3)
  val dest: String = fields(4)
  val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
  val dep_delay: Double = fields(6).toDouble
  val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
  val arv_delay: Double = fields(8).toDouble
  val airtime: Double = fields(9).toDouble
  val distance: Double = fields(10).toDouble
  
  Flight(date,airline,flightnum,origin,dest,dep,
         dep_delay,arv,arv_delay,airtime,distance)

    }


```


```scala
val flights=sc.textFile(flightsPath)
```


```scala
// The total number of records 
flights.count()
```




476881




```scala
// The first row
flights.first()
```




2014-04-01,19805,1,JFK,LAX,0854,-6.00,1217,2.00,355.00,2475.00




```scala
flights.map(_.split(","))
```




MapPartitionsRDD[6] at map at <console>:25




```scala
flights.map(x => x.split(","))
```




MapPartitionsRDD[7] at map at <console>:25




```scala
val flightsParsed=flights.map(parse)
```


```scala
// Let's take a look at the data in the Parsed RDD 
flightsParsed.first()
```




Flight(2014-01-01,19805,1,JFK,LAX,08:54:00.000,-6.0,12:17:00.000,2.0,355.0,2475.0)




```scala
val totalDistance=flightsParsed.map(_.distance).reduce((x,y) => x+y)
```


```scala
val avgDistance=totalDistance/flightsParsed.count()
```


```scala
println(avgDistance)
```

794.8585013871385



```scala
// % flights with delays
flightsParsed.filter(_.dep_delay>0).count().toDouble/flightsParsed.count().toDouble
```




0.3753871510922012




```scala
flightsParsed.persist()
```




MapPartitionsRDD[38] at map at <console>:272




```scala
val sumCount=flightsParsed.map(_.dep_delay).aggregate((0.0,0))((acc, value) => (acc._1 + value, acc._2+1),
                                                           (acc1,acc2) => (acc1._1+acc2._1,acc1._2+acc2._2))


```


```scala
sumCount._1/sumCount._2
```



8.313877046894298




```scala
sumCount.getClass
```




class scala.Tuple2$mcDI$sp




```scala
// Histogram of delays
flightsParsed.map(x => (x.dep_delay/60).toInt).countByValue()
```




Map(0 -> 452963, 5 -> 249, 10 -> 15, 24 -> 3, 25 -> 1, 14 -> 13, 20 -> 4, 1 -> 16016, 6 -> 113, 28 -> 1, 21 -> 3, 9 -> 26, 13 -> 15, 2 -> 4893, 17 -> 2, 12 -> 9, 7 -> 66, 3 -> 1729, 11 -> 12, 8 -> 43, 4 -> 701, 15 -> 4)




```scala
val airportDelays = flightsParsed.map(x => (x.origin,x.dep_delay))
```


```scala
airportDelays.keys.take(10)
```




Array(JFK, LAX, JFK, LAX, DFW, OGG, DFW, HNL, JFK, LAX)




```scala
airportDelays.values.take(10)
```




Array(-6.0, 14.0, -6.0, 25.0, -5.0, 126.0, 125.0, 4.0, -7.0, 21.0)




```scala
val airportTotalDelay=airportDelays.reduceByKey((x,y) => x+y)
```


```scala
val airportCount=airportDelays.mapValues(x => 1).reduceByKey((x,y) => x+y)
```


```scala
val airportSumCount=airportTotalDelay.join(airportCount)
```


```scala
val airportAvgDelay=airportSumCount.mapValues(x => x._1/x._2.toDouble)
```


```scala
airportAvgDelay.sortBy(-_._2).take(10)
```




   Array((PPG,56.25), (EGE,32.0), (OTH,24.533333333333335), (LAR,18.892857142857142), (RDD,18.55294117647059), (MTJ,18.363636363636363), (PUB,17.54), (EWR,16.478549005929544), (CIC,15.931034482758621), (RST,15.6993006993007))




```scala
val airportSumCount2=airportDelays.combineByKey(
                                            value => (value,1),
                                            (acc: (Double,Int), value) =>  (acc._1 + value, acc._2+1),
                                            (acc1: (Double,Int), acc2: (Double,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2))
```


```scala
val airportAvgDelay2=airportSumCount2.mapValues(x => x._1/x._2.toDouble)
```


```scala
airportAvgDelay2.sortBy(-_._2).take(10)
```




   Array((PPG,56.25), (EGE,32.0), (OTH,24.533333333333335), (LAR,18.892857142857142), (RDD,18.55294117647059), (MTJ,18.363636363636363), (PUB,17.54), (EWR,16.478549005929544), (CIC,15.931034482758621), (RST,15.6993006993007))




```scala
val airports=sc.textFile(airportsPath).filter(notHeader).map(parseLookup)
```


```scala
airports.lookup("PPG")
```




   WrappedArray(Pago Pago)




```scala
val airportLookup=airports.collectAsMap
```


```scala
airportLookup("CLI")
```




   Clintonville




```scala
airportAvgDelay.map(x=>(airportLookup(x._1),x._2)).take(10)
```




   Array((Santa Maria,5.285714285714286), (Wichita Falls,8.717948717948717), (Manhattan/Ft. Riley,3.9705882352941178), (Bloomington/Normal,4.86), (Helena,-2.048076923076923), (Sun Valley/Hailey/Ketchum,-4.408163265306122), (Richmond,8.803352675693102), (Ponce,-0.8103448275862069), (Salt Lake City,3.5174873446847674), (New Bern/Morehead/Beaufort,5.660714285714286))




```scala
val airportBC=sc.broadcast(airportLookup)
```


```scala
airportAvgDelay.map(x => (airportBC.value(x._1),x._2)).sortBy(-_._2).take(10)
```




   Array((Pago Pago,56.25), (Eagle,32.0), (North Bend/Coos Bay,24.533333333333335), (Laramie,18.892857142857142), (Redding,18.55294117647059), (Montrose/Delta,18.363636363636363), (Pueblo,17.54), (Newark,16.478549005929544), (Chico,15.931034482758621), (Rochester,15.6993006993007))


