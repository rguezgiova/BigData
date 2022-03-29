## Ejercicio: Comenzando con los RDDs

### A - Exploración de fichero plano 1

Crea un RDD llamado “relato” que contenga el contenido del fichero utilizando el método “textFile”

```
val relato = sc.textFile("file:/home/cloudera/BIT/data/relato.txt")
relato: org.apache.spark.rdd.RDD[String] = file:/home/cloudera/BIT/data/relato.txt MapPartitionsRDD[3] at textFile at <console>:27
```

Cuenta el número de líneas del RDD y observa el resultado. Si el resultado es 23 es correcto.

```
relato.count()
res1: Long = 23
```

Ejecuta el método “collect()” sobre el RDD y observa el resultado. Recuerda lo que comentamos durante el curso sobre cuándo es recomendable el uso de este método.

```
relato.collect()
res2: Array[String] = Array(Two roads diverged in a yellow wood,, And sorry I could not travel both, And be one traveler, long I stood, And looked down one as far as I could, To where it bent in the undergrowth;, "", Then took the other, as just as fair,, And having perhaps the better claim,, Because it was grassy and wanted wear;, Though as for that the passing there, Had worn them really about the same,, "", And both that morning equally lay, In leaves no step had trodden black., Oh, I kept the first for another day!, Yet knowing how way leads on to way,, I doubted if I should ever come back., "", I shall be telling this with a sigh, Somewhere ages and ages hence:, Two roads diverged in a wood, and I--, I took the one less traveled by,, And that has made all the difference.)
```

Investiga cómo usar la función “foreach” para visualizar el contenido del RDD de una forma más cómoda de entender.

```
relato.foreach(f => println(f))
Two roads diverged in a yellow wood,

And sorry I could not travel both
And be one traveler, long I stood
And looked down one as far as I could
To where it bent in the undergrowth;

And both that morning equally lay
Then took the other, as just as fair,
In leaves no step had trodden black.
And having perhaps the better claim,
Oh, I kept the first for another day!
Because it was grassy and wanted wear;
Yet knowing how way leads on to way,
Though as for that the passing there
I doubted if I should ever come back.

I shall be telling this with a sigh
Somewhere ages and ages hence:
Had worn them really about the same,
Two roads diverged in a wood, and I--
I took the one less traveled by,
And that has made all the difference.
```

### B- Exploración de fichero plano 2

Crea una variable que contenga la ruta del fichero, por ejemplo file:/home/BIT/data/weblogs/2013-09-15.log.

```
val ruta = "file:/home/cloudera/BIT/data/weblogs/2013-09-15.log"
ruta: String = file:/home/cloudera/BIT/data/weblogs/2013-09-15.log
```

Crea un RDD con el contenido del fichero llamado logs.

```
val logs = sc.textFile(ruta)
logs: org.apache.spark.rdd.RDD[String] = file:/home/cloudera/BIT/data/weblogs/2013-09-15.log MapPartitionsRDD[5] at textFile at <console>:29
```

Crea un nuevo RDD, jpglogs, que contenga solo las líneas del RDD que contienen la cadena de caracteres “.jpg”. Puedes usar el método contains().

```
val jpglogs = logs.filter(file => file.contains(".jpg"))
jpglogs: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[6] at filter at <console>:31
```

Imprime en pantalla las 5 primeras líneas de jpglogs.

```
jpglogs.take(5)
res6: Array[String] = Array(233.19.62.103 - 16261 [15/Sep/2013:23:55:57 +0100] "GET /titanic_1100.jpg HTTP/1.0" 200 16713 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F10L", 136.98.55.4 - 22232 [15/Sep/2013:23:52:49 +0100] "GET /ifruit_1.jpg HTTP/1.0" 200 7127 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 2", 219.248.10.104 - 66652 [15/Sep/2013:23:49:59 +0100] "GET /ifruit_4a.jpg HTTP/1.0" 200 1686 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F20L", 47.148.67.112 - 40002 [15/Sep/2013:23:44:07 +0100] "GET /meetoo_5.1.jpg HTTP/1.0" 200 6804 "http://www.loudacre.com"  "Loudacre Mobile Browser Titanic 2200", 29.110.195.184 - 39859 [15/Sep/2013:23:42:32 +0100] "GET /titanic_2400.jpg HTTP/1.0" 200 4175 "http://www.loudacre.com"  "Loudacre Mobile Br...
```

Es posible anidar varios métodos en la misma línea. Crea una variable jpglogs2 que devuelva el número de líneas que contienen la cadena de caracteres “.jpg”.

```
val jpglogs = logs.filter(file => file.contains(".jpg")).count()
jpglogs: Long = 237
```

Ahora vamos a comenzar a usar una de las funciones más importantes de Spark, la función “map()”. Para ello, coge el RDD logs y calcula la longitud de las 5 primeras líneas. Puedes usar la función “size()” o “length()” Recordad que la función map ejecuta una función sobre cada línea del RDD, no sobre el conjunto total del RDD.

```
logs.map(file => file.length).take(5)
res0: Array[Int] = Array(141, 134, 140, 133, 143)
```

Imprime por pantalla cada una de las palabras que contiene cada una de las 5 primeras líneas del RDD logs. Puedes usar la función “split()”.

```
logs.map(file => file.split(" ")).take(5)
res1: Array[Array[String]] = Array(Array(116.180.70.237, -, 128, [15/Sep/2013:23:59:53, +0100], "GET, /KBDOC-00031.html, HTTP/1.0", 200, 1388, "http://www.loudacre.com", "", "Loudacre, CSR, Browser"), Array(116.180.70.237, -, 128, [15/Sep/2013:23:59:53, +0100], "GET, /theme.css, HTTP/1.0", 200, 5531, "http://www.loudacre.com", "", "Loudacre, CSR, Browser"), Array(218.193.16.244, -, 94, [15/Sep/2013:23:58:45, +0100], "GET, /KBDOC-00273.html, HTTP/1.0", 200, 5325, "http://www.loudacre.com", "", "Loudacre, CSR, Browser"), Array(218.193.16.244, -, 94, [15/Sep/2013:23:58:45, +0100], "GET, /theme.css, HTTP/1.0", 200, 9463, "http://www.loudacre.com", "", "Loudacre, CSR, Browser"), Array(198.122.118.164, -, 131, [15/Sep/2013:23:58:02, +0100], "GET, /KBDOC-00117.html, HTTP/1.0", 200, 15818, "htt...
```

Mapea el contenido de logs a un RDD “logwords” de arrays de palabras de cada línea.

```
val logwords = logs.map(line => line.split(" "))
logwords: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[8] at map at <console>:31
```

### C- Exploración de un conjunto de ficheros planos en una carpeta

Crea un nuevo RDD llamado “ips” a partir del RDD logs que contenga solamente las ips de cada línea (primer elemento de cada fila).

```
val ips = logs.map(line => line.split(" ")(0))
ips: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[9] at map at <console>:31
```

Imprime por pantalla las 5 primeras líneas de ips

```
ips.take(5)
res2: Array[String] = Array(116.180.70.237, 116.180.70.237, 218.193.16.244, 218.193.16.244, 198.122.118.164)
```

Visualiza el contenido de ips con la función “collect()”. Verás que no es demasiado intuitivo. Prueba a usar el comando “foreach”

```
ips.collect()
res3: Array[String] = Array(116.180.70.237, 116.180.70.237, 218.193.16.244, 218.193.16.244, 198.122.118.164, 198.122.118.164, 103.17.173.248, 103.17.173.248, 233.19.62.103, 233.19.62.103, 233.19.62.103, 233.19.62.103, 94.72.13.242, 94.72.13.242, 136.98.55.4, 136.98.55.4, 136.98.55.4, 136.98.55.4, 199.198.44.247, 199.198.44.247, 45.212.146.190, 45.212.146.190, 219.248.10.104, 219.248.10.104, 219.248.10.104, 219.248.10.104, 26.79.166.169, 26.79.166.169, 176.80.85.20, 176.80.85.20, 32.130.214.151, 32.130.214.151, 44.106.84.58, 44.106.84.58, 191.197.49.235, 191.197.49.235, 47.148.67.112, 47.148.67.112, 47.148.67.112, 47.148.67.112, 249.33.231.34, 249.33.231.34, 29.110.195.184, 29.110.195.184, 29.110.195.184, 29.110.195.184, 105.11.140.241, 105.11.140.241, 105.180.63.72, 105.180.63.72, 4.75....
```

```
ips.foreach(ip => println(ip))
177.181.50.189
177.181.50.189
250.164.5.253
250.164.5.253
166.63.228.163
166.63.228.163
24.65.98.2
24.65.98.2
24.65.98.2
24.65.98.2
54.98.159.136
54.98.159.136
207.16.167.36
207.16.167.36
206.71.97.136
206.71.97.136
179.28.218.65
179.28.218.65
179.28.218.65
179.28.218.65
```

Crea un bucle “for” para visualizar el contenido de las 10 primeras líneas de ips.

```
for(ip <- ips.take(10)){print(ip+"\n")}
116.180.70.237
116.180.70.237
218.193.16.244
218.193.16.244
198.122.118.164
198.122.118.164
103.17.173.248
103.17.173.248
233.19.62.103
233.19.62.103
```

Guarda el contenido de “ips” entero en un fichero de texto usando el método saveAsTextFile en la ruta “/home/cloudera/iplist” y observa su contenido.

```
ips.saveAsTextFile("file:/home/cloudera/iplist")
```

Crea un RDD que contenga solo las ips de todos los documentos contenidos en la ruta “/home/BIT/data/weblogs”. Guarda su contenido en la ruta “/home/cloudera/iplists” y observa su contenido.

```
val ips = sc.textFile("file:/home/cloudera/BIT/data/weblogs").map(file => file.split(" ")(0))
ips: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[13] at map at <console>:27
```

```
ips.saveAsTextFile("file:/home/cloudera/iplists")
[Stage 9:===>                                                    (11 + 2) / 182
[Stage 9:=====>                                                  (19 + 2) / 182
[Stage 9:========>                                               (29 + 2) / 182
[Stage 9:===========>                                            (37 + 2) / 182
[Stage 9:=============>                                          (43 + 2) / 182
[Stage 9:================>                                       (53 + 2) / 182
[Stage 9:==================>                                     (61 + 2) / 182
[Stage 9:=====================>                                  (70 + 2) / 182
[Stage 9:========================>                               (81 + 2) / 182
[Stage 9:===========================>                            (89 + 2) / 182
[Stage 9:=============================>                          (97 + 2) / 182
[Stage 9:================================>                      (108 + 2) / 182
[Stage 9:===================================>                   (118 + 2) / 182
[Stage 9:======================================>                (129 + 2) / 182
[Stage 9:=========================================>             (136 + 2) / 182
[Stage 9:============================================>          (146 + 2) / 182
[Stage 9:===============================================>       (156 + 2) / 182
[Stage 9:==================================================>    (167 + 2) / 182
[Stage 9:=====================================================> (176 + 2) / 182
```

A partir del RDD logs, crea un RDD llamado “htmllogs” que contenga solo la ip seguida de cada ID de usuario de cada fichero html. El ID de usuario es el tercer campo de cada línea de cada log. Después imprime las 5 primeras líneas.

```
val htmllogs = logs.filter(_.contains(".html")).map(line => (line.split(" ")(0), line.split(" ")(2)))
htmllogs: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[8] at map at <console>:31```
```

```
htmllogs.take(5).foreach(f => println(f._1 + " - " + f._2))
```

## Ejercicio: Trabajando con PairRDDs

### A- Trabajo con todos los datos de la carpeta de logs: "/home/BIT/data/weblogs"

Usando MapReduce, cuenta el número de peticiones de cada usuario, es decir, las veces que cada usuario aparece en una línea de un log. Para ello:

a. Usa un Map para crear un RDD que contenga el par (ID, 1), siendo la clave el ID y el Value el número 1. Recordad que el campo ID es el tercer elemento de cada línea.

```
val requests = logs.map(line => line.split(" ")).map(word => (word (2), 1))
requests: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[6] at map at <console>:31
```

b. Usa un Reduce para sumar los valores correspondientes a cada userid.

```
val reducerequests = requests.reduceByKey((v1, v2) => v1 + v2)
reducerequests: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[8] at reduceByKey at <console>:33
```

Muestra los ids de usuario y el número de accesos para los 10 usuarios con mayor número de accesos. Para ello:

a. Utiliza un map() para intercambiar la Clave por el Valor.

```
val swaprequests = reducerequests.map(line => line.swap)
swaprequests: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[9] at map at <console>:35
```

Utiliza la función vista en teoría para ordenar un RDD. Ten en cuenta que queremos mostrar los datos en orden descendiente (De mayor a menor número de peticiones). Recuerda que el RDD debe estar en la misma forma que al inicio, es decir, con clave: userid y valor: nº de peticiones.

```
swaprequests.sortByKey(false).map(line => line.swap).take(10).foreach(println)
(193,1603)                                                                      
(77,1547)
(119,1540)
(34,1526)
(182,1524)
(64,1508)
(189,1508)
(20,1502)
(173,1500)
(17,1500)
```

Crea un RDD donde la clave sea el userid y el valor sea una lista de ips a las que el userid se ha conectado (es decir, agrupar las IPs por userID). Ayúdate de la función groupByKey() para conseguirlo.

```
val userips = logs.map(line => line.split(" ")).map(word => (word(2), word(0))).groupByKey()
userips: org.apache.spark.rdd.RDD[(String, Iterable[String])] = ShuffledRDD[16] at groupByKey at <console>:31
```

```
userips.take(10)
res3: Array[(String, Iterable[String])] = Array((79844,CompactBuffer(136.132.254.160, 136.132.254.160, 53.251.68.51, 53.251.68.51)), (16669,CompactBuffer(23.137.191.64, 23.137.191.64)), (99640,CompactBuffer(207.61.107.245, 207.61.107.245, 17.159.12.204, 17.159.12.204, 96.24.214.109, 96.24.214.109, 123.79.96.8, 123.79.96.8, 20.117.86.221, 20.117.86.221, 142.96.254.175, 142.96.254.175, 35.107.69.206, 35.107.69.206, 208.153.228.87, 208.153.228.87, 67.92.22.4, 67.92.22.4, 15.209.169.137, 15.209.169.137, 15.209.169.137, 15.209.169.137, 51.239.242.13, 51.239.242.13, 51.239.242.13, 51.239.242.13, 18.76.240.35, 18.76.240.35, 107.125.111.173, 107.125.111.173, 245.141.100.16, 245.141.100.16, 215.150.177.226, 215.150.177.226, 215.150.177.226, 215.150.177.226, 39.175.103.131, 39.175.103.131, 102.17...
```

### B- Trabajo con todos los datos de la carpeta de logs: "/home/BIT/data/accounts.cvs"

Haz un JOIN entre los datos de logs del ejercicio pasado y los datos de accounts.csv, de manera que se obtenga un conjunto de datos en el que la clave sea el userid y como valor tenga la información del usuario seguido del número de visitas de cada usuario. Los pasos a ejecutar son:

a. Haz un map() de los datos de accounts.cvs de forma que la Clave sea el userid y el Valor sea toda la línea, incluido el userid.

```
scala> val accounts = sc.textFile(file).map(line => line.split(',')).map(account => (account(0), account))
accounts: org.apache.spark.rdd.RDD[(String, Array[String])] = MapPartitionsRDD[3] at map at <console>:29
```

b. Haz un JOIN del RDD que acabas de crear con el que creaste en el paso anterior que contenía (userid, nº visitas).

```
scala> val userviews = accounts.join(requests)
userviews: org.apache.spark.rdd.RDD[(String, (Array[String], Iterable[String]))] = MapPartitionsRDD[23] at join at <console>:37
```

c. Crea un RDD a partir del RDD anterior, que contenga el userid, número de visitas, nombre y apellido de las 5 primeras líneas.

```
scala> for (field <- userviews.take(5)){println(field._1, field._2._2, field._2._1(3), field._2._1(4))}
22/03/29 12:48:24 WARN memory.TaskMemoryManager: leak 89.4 MB memory from org.apache.spark.util.collection.ExternalAppendOnlyMap@40f6673
22/03/29 12:48:24 ERROR executor.Executor: Managed memory leak detected; size = 93741254 bytes, TID = 6
(22620,1,Matthew,Gaskin)
(22620,1,Matthew,Gaskin)
(178,1,Kimberly,Mulder)
(178,1,Kimberly,Mulder)
(178,1,Kimberly,Mulder)
```

### C- Trabajo con más métodos sobre pares RDD

Usa keyBy para crear un RDD con los datos de las cuentas, pero con el código postal como clave (noveno campo del fichero accounts.CSV).

```
scala> val accountscp = sc.textFile(file).map(line => line.split(",")).keyBy(_(8))
accountscp: org.apache.spark.rdd.RDD[(String, Array[String])] = MapPartitionsRDD[20] at keyBy at <console>:29
```

Crea un RDD de pares con el código postal como la clave y una lista de nombres (Apellido, Nombre) de ese código postal como el valor. Sus lugares son el 5º y el 4º respectivamente.

```
scala> val accountsname = accountscp.mapValues(value => value(4) + ", " + value(3)).groupByKey()
accountsname: org.apache.spark.rdd.RDD[(String, Iterable[String])] = ShuffledRDD[22] at groupByKey at <console>:31
```

Ordena los datos por código postal y luego, para los primeros 5 códigos postales, muestra el código y la lista de nombres cuyas cuentas están en ese código postal.

```
scala> accountsname.sortByKey().take(5).foreach{field => println("--- " + field._1); field._2.foreach(println)}
--- 85000
Willson, Leon
Clark, Ronald
Rush, Juanita
Woodhouse, Roger
Baptist, Colin
--- 85002
Whitmore, Alan
Chandler, Tara
Robinson, Diane
Brown, Henry
Sisson, Lacey
--- 85004
Morris, Eric
Reiser, Hazel
Gregg, Alicia
Preston, Elizabeth
Hass, Julie
```

## Ejercicios opcionales: Trabajando con PairRDDs

### EJ1: Tareas a realizar

Utilizando la terminal, introduce dicho dataset en el HDFS.

```bash

```