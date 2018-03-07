# tp spark  jour 1 

## Chargement du spark shell et import des fichiers dans spark
```
sudo su spark
cd /usr/hdp/current/spark2-client/
hdfs dfs -copyFromLocal /etc/hadoop/conf/log4j.properties /tmp/data.txt
val data = spark.read.textFile("/tmp/data.txt").as[String]
val data = spark.read.textFile("hdfs://user/tp-hive/julie/data.csv").as[String] // nope
val bibli = sc.textFile("hdfs://user/tp-hive/julie/data.csv")
```
Traitement + action :
```
val words = data.flatMap(value => value.split("\\s+"))
val groupedWords = words.groupByKey(_.toLowerCase)
val counts = groupedWords.count()
// val counts = data.flatMap(line => line.split(";")).map(word => (word, 1)).reduceByKey(_ + _)
```

## Affichage des résultats
```
counts.show()
counts.count() //longueur du resultat
counts.rdd.coalesce(1).saveAsTextFile("PATH/res_wc.csv")
counts.rdd.coalesce(1).saveAsTextFile("/tpspark/julie/res_wc.csv")
counts.rdd.saveAsTextFile("/test/res_wc.csv")
val linesWithlivre = textFile.filter(line => line.contains("Livre"))
//val counts2 = groupedWords.reduceByKey(_ + _)
```

## Lister les types exacts des objets suivants :
```
spark.read.textFile("/tmp/data.txt")
spark.read.textFile("/tmp/data.txt").as[String]
data.flatMap()
words.groupByKey()
groupedWords.count()

val bibli = sc.textFile("hdfs://user/tp-hive/julie/data.csv") // string
val counts = data.flatMap(line => line.split(";")).map(word => (word, 1)) // c'est une liste : [_1: string, _2: int]
val counts_agrege = counts.groupByKey(identity).count()  // Dataset[((String, Int), Long)] = [key: struct<_1: string, _2: int>, count(1): bigint]
```

# jour 2 ( parallelize à voir) 
## RDD :
```
val bibli = sc.textFile("hdfs://user/tp-hive/julie/data.csv") // RDD string
val bibli = spark.read.textFile("/user/tp-hive/julie/data.csv").as[String]   // ok dataset

val words1 = bibli.flatMap(line => line.split(" ")).map(word => (word, 1)) // [(String, Int)] = MapPartitionsRDD
val words2 = bibli.flatMap(value => value.split("\\s+")) // RDD[String] = MapPartitionsRDD[10]
val words3 = bibli.flatMap(line => line.split(";")).map(word => (word, 1)) // [(String, Int)]
val words2 = bibli.flatMap(value => value.split("\\s+")) 

val groupedWords1 = words1.groupByKey(_.toLowerCase) // nope marche pas avec RDD
val groupedWords2 = words2.groupByKey(_.toLowerCase) // nope
val groupedWords3 = words1.groupByKey(identity).count()  // nope
```
## from dataset
```
val data = spark.read.textFile("/tmp/bibli.csv").as[String] // dataset string
val words = data.flatMap(value => value.split("\\s+")) /// .Dataset[String] = [value: string]
val groupedWords = words.groupByKey(_.toLowerCase) /// roupedDataset[String,String] 
val counts = groupedWords.count() ///[(String, Long)]
counts.show()

val linesWithLivre = data.filter(line => line.contains("Livre"))/// .Dataset[String] = [value: string]

counts.rdd.coalesce(1).saveAsTextFile("/user/tp-spark/julie/bibli_count.csv")
linesWithLivre.rdd.coalesce(1).saveAsTextFile("/user/tp-spark/julie/bibli_filter.csv")
```

# Spark sql
## Définition du contexte et chargement des fichiers
```
val sqlcontext = new org.apache.spark.sql.SQLContext(sc) // 
//  spark.read.option("header", "true").option("inferSchema", "true").csv("/user/pdg/testhive/nba.csv")
// val df1 = sqlcontext.read.format("csv").option("header", "true").option("delimiter", ";").load("/tmp/bibli.csv")
val df = sqlcontext.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").load("/user/tp-hive/julie/data.csv") 
df.show() // montre les 20 premières lignes du dataframe
```
## Montre le schéma du dataframe
```
scala> df.printSchema()
root
 |-- Anne: integer (nullable = true)
 |-- Relais: string (nullable = true)
 |-- Typedoc : string (nullable = true)
 |-- nombreprts: integer (nullable = true)
 |-- id__: string (nullable = true)
```
## Afficher le début du dataframe only showing top 3 rows
```
scala>  df.show(3)
+----+--------------------+--------------------+----------+--------------------+
|Anne|              Relais|            Typedoc |nombreprts|                id__|
+----+--------------------+--------------------+----------+--------------------+
|2015|1022107 Langrolay...|LIV Livre        ...|       818|00012caa-125d-476...|
|2015|1022033 Caulnes  ...|CD Compact Disc  ...|       351|0007e245-4194-40b...|
|2009|1022047 Corlay   ...|LCD Livre disque ...|        32|0009819b-3ad8-456...|
+----+--------------------+--------------------+----------+--------------------+
```

## select
```
scala> df.select("Anne").show(5)
+----+
|Anne|
+----+
|2015|
|2015|
|2009|
|2015|
|2015|
+----+
```
## filtre supérieur à 
```
scala>  df.filter(df("nombreprts") > 800).show(10)
+----+--------------------+--------------------+----------+--------------------+
|Anne|              Relais|            Typedoc |nombreprts|                id__|
+----+--------------------+--------------------+----------+--------------------+
|2015|1022107 Langrolay...|LIV Livre        ...|       818|00012caa-125d-476...|
|2017|    1022046 Collin�e|               TOTAL|      1078|0041407d-e4b8-435...|
|2007|1022047 Corlay   ...|TOTAL            ...|      1319|006e98fb-bea5-45d...|
|2014|1022185 Pl�n�e-Ju...|TOTAL            ...|      1539|00b3eff0-dc8a-4b7...|
|2007|1022275 St Barnab...|LIV Livre        ...|       913|00b3f739-d5d7-41c...|
|2007|1022340 Tonqu�dec...|TOTAL            ...|      1229|0104cd2b-86ac-41b...|
|2010|1022116 Lanrodec ...|TOTAL            ...|       894|010b9271-8eb3-4ef...|
|2016|    1022164 P�dernec|           LIV Livre|      1563|010d2fc3-6f1b-459...|
|2015|1022217 Plougras ...|TOTAL            ...|       990|0149dd73-0c6f-4d2...|
|2008|1022327 St Samson...|TOTAL            ...|       985|0158fbcf-97b4-4d7...|
+----+--------------------+--------------------+----------+--------------------+
```

## filtre supérieur not equal and contains
```
df.filter($"Typedoc " !== "yolo").show(10)

df.filter($"Typedoc " contains "Livre").show(5)
+----+--------------------+--------------------+----------+--------------------+
|Anne|              Relais|            Typedoc |nombreprts|                id__|
+----+--------------------+--------------------+----------+--------------------+
|2015|1022107 Langrolay...|LIV Livre        ...|       818|00012caa-125d-476...|
|2009|1022047 Corlay   ...|LCD Livre disque ...|        32|0009819b-3ad8-456...|
|2015|1022113 Lannebert...|LIV Livre        ...|         0|000cff37-b873-45a...|
|2012|1022233 Plourhan ...|LIV Livre        ...|       645|00115ee8-d00f-47e...|
|2013|1022061 Evran    ...|LCD Livre disque ...|         6|001d1468-32a0-4da...|
+----+--------------------+--------------------+----------+--------------------+

scala> df.filter($"Typedoc " contains "Livre").filter($"nombreprts" > 800).show(5)
+----+--------------------+--------------------+----------+--------------------+
|Anne|              Relais|            Typedoc |nombreprts|                id__|
+----+--------------------+--------------------+----------+--------------------+
|2015|1022107 Langrolay...|LIV Livre        ...|       818|00012caa-125d-476...|
|2007|1022275 St Barnab...|LIV Livre        ...|       913|00b3f739-d5d7-41c...|
|2016|    1022164 P�dernec|           LIV Livre|      1563|010d2fc3-6f1b-459...|
|2011|1022209 Ploubalay...|LIV Livre        ...|      2531|016603e7-ff26-4a2...|
|2008|1022346 Tr�daniel...|LIV Livre        ...|      1300|023cd127-090e-483...|
+----+--------------------+--------------------+----------+--------------------+

scala> df.filter($"Typedoc " contains "Livre").filter($"nombreprts" > 5).show(5)
+----+--------------------+--------------------+----------+--------------------+
|Anne|              Relais|            Typedoc |nombreprts|                id__|
+----+--------------------+--------------------+----------+--------------------+
|2015|1022107 Langrolay...|LIV Livre        ...|       818|00012caa-125d-476...|
|2009|1022047 Corlay   ...|LCD Livre disque ...|        32|0009819b-3ad8-456...|
|2012|1022233 Plourhan ...|LIV Livre        ...|       645|00115ee8-d00f-47e...|
|2013|1022061 Evran    ...|LCD Livre disque ...|         6|001d1468-32a0-4da...|
|2015|1022263 Qu�vert  ...|LCD Livre disque ...|         7|00210c87-1840-499...|
+----+--------------------+--------------------+----------+--------------------+
```
## filtre et groupBy et count
```
scala> df.filter($"Typedoc " contains "Livre").groupBy("Typedoc ").count().show()
+--------------------+-----+
|            Typedoc |count|
+--------------------+-----+
|LCD Livre disque ...| 1205|
|LIV Livre        ...| 2832|
|  LCA Livre cassette|    2|
|LCA Livre cassett...|  115|
|           LIV Livre|  584|
|LCD Livre disque ...|  282|
+--------------------+-----+
```
## filtre, groupBy et somme
```
scala> df.filter($"Typedoc " contains "Livre").groupBy("Typedoc ").sum().show()
+--------------------+---------+---------------+
|            Typedoc |sum(Anne)|sum(nombreprts)|
+--------------------+---------+---------------+
|LCD Livre disque ...|  2423449|          16723|
|LIV Livre        ...|  5695040|        1593646|
|  LCA Livre cassette|     4033|              2|
|LCA Livre cassett...|   230982|            324|
|           LIV Livre|  1177636|         266072|
|LCD Livre disque ...|   568653|           3226|
+--------------------+---------+---------------+

scala> df.select("Typedoc ", "nombreprts").filter($"Typedoc " contains "Livre").groupBy("Typedoc ").sum().show
+--------------------+---------------+
|            Typedoc |sum(nombreprts)|
+--------------------+---------------+
|LCD Livre disque ...|          16723|
|LIV Livre        ...|        1593646|
|  LCA Livre cassette|              2|
|LCA Livre cassett...|            324|
|           LIV Livre|         266072|
|LCD Livre disque ...|           3226|
+--------------------+---------------+

```
## filtre groupBy et sort
```
scala> df.select("Typedoc ", "nombreprts").filter($"Typedoc " contains "Livre").groupBy("Typedoc ").sum().sort(desc("Typedoc ")).show / .sort($"Typedoc ".desc)
+--------------------+---------------+
|            Typedoc |sum(nombreprts)|
+--------------------+---------------+
|LIV Livre        ...|        1593646|
|           LIV Livre|         266072|
|LCD Livre disque ...|          16723|
|LCD Livre disque ...|           3226|
|LCA Livre cassett...|            324|
|  LCA Livre cassette|              2|
+--------------------+---------------+
```

## Jointure

```
scala> val dfb = df.drop($"nombreprts").drop($"id__")
scala> val dfa = df.drop($"Anne").drop($"Typedoc")

scala> val joinedDF = dfa.join(dfb, dfa("Relais") === dfb("Relais"), "inner")
18/03/06 14:35:43 WARN Column: Constructing trivially true equals predicate, 'Relais#641 = Relais#641'. Perhaps you need to use aliases.
joinedDF: org.apache.spark.sql.DataFrame = [Relais: string, nombreprts: int ... 4 more fields]

scala> joinedDF.printSchema  //// 2 fois relais ambiguous
root
 |-- Relais: string (nullable = true)
 |-- nombreprts: integer (nullable = true)
 |-- id__: string (nullable = true)
 |-- Anne: integer (nullable = true)
 |-- Relais: string (nullable = true)
 |-- Typedoc : string (nullable = true)

 
scala> val joinedDF = dfa.as('a).join(dfb.as('b), $"a.Relais" === $"b.Relais")
joinedDF: org.apache.spark.sql.DataFrame = [Relais: string, nombreprts: int ... 4 more fields]

scala> joinedDF.printSchema() // not ambiguous
root
 |-- Relais: string (nullable = true)
 |-- nombreprts: integer (nullable = true)
 |-- id__: string (nullable = true)
 |-- Anne: integer (nullable = true)
 |-- Relais: string (nullable = true)
 |-- Typedoc : string (nullable = true)
 
 scala> val joinedDF = dfa.join(dfb, "Relais")
 ```
## pas de colonne en double
```
 scala> joinedDF.printSchema()  
 root
 |-- Relais: string (nullable = true)
 |-- nombreprts: integer (nullable = true)
 |-- id__: string (nullable = true)
 |-- Anne: integer (nullable = true)
 |-- Typedoc : string (nullable = true)
```

## commande jar
à tester : a=df.CreateGlobalTempView  ensuite spark.sql(a ...) 
```
cd /usr/hdp/current/spark2-client/bin
spark-submit --class sparkflair.Wordcount  --master yarn /home/ubuntu/SparkJob2.jar  // infini
spark-submit --class sparkflair.Wordcount --master yarn --deploy-mode cluster /home/spark/SparkJob2.jar
```
