

```python
val googlePath="web-Google.txt"
val googleWeblinks=sc.textFile(googlePath).filter(!_.contains("#")).map(_.split("\t")).map(x => (x(0),x(1)))
```


```python
val links = googleWeblinks.partitionBy(new HashPartitioner(100)).groupByKey.cache()

var ranks = links.mapValues(v => 1.0)
val iters = 2

for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

ranks.sortBy(-_._2).take(10)
    
```




   Array((163075,639.2815445099519), (597621,625.4610600036211), (537039,607.9168836929598), (885605,595.209230632999), (41909,583.9895718071381), (837478,573.3176083333332), (551829,551.3895311258591), (605856,551.3305802790813), (504140,537.3738795157586), (407610,492.568315758002))


