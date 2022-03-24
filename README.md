# Spark

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

Usando MapReduce, cuenta el número de peticiones de cada usuario, es decir, las veces que cada usuario aparece en una línea de un log. Para ello:

A) Usa un Map para crear un RDD que contenga el par (ID, 1), siendo la clave el ID y el Value el número 1. Recordad que el campo ID es el tercer elemento de cada línea.

```

```