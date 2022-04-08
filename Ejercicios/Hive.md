Entrar en Hive.

```
[cloudera@quickstart ~]$ hive
Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
```

Modificar la propiedad correspondiente para mostrar por pantalla las cabeceras de las tablas.

```
hive> set hive.cli.print.header = true;
```

Crear una base de datos llamada “cursohivedb”.

```
hive> CREATE DATABASE cursohivedb;
OK
Time taken: 2.118 seconds
```

Situarnos en la base de datos recién creada para trabajar con ella.

```
hive> USE cursohivedb;
OK
Time taken: 0.161 seconds
```

Comprobar que la base de datos está vacía.

```
hive> SHOW TABLES;
OK
tab_name
Time taken: 0.309 seconds
```

Crear una tabla llamada “iris” en nuestra base de datos que contenga 5 columnas (s_length float,s_width float,p_length float,p_width float,clase string) cuyos campos estén separados por comas (ROW FORMAT DELIMITED FIELDS TERMINATED BY ',')

```
hive> CREATE TABLE iris (
    > s_lenght float,
    > s_width float,
    > p_length float,
    > p_width float,
    > clase string
    > ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
OK
Time taken: 0.409 seconds
```

Comprobar que la tabla se ha creado y el tipado de sus columnas.

```
hive> SHOW TABLES;
OK
tab_name
iris
Time taken: 0.095 seconds, Fetched: 1 row(s)
```

```
hive> DESCRIBE iris;
OK
col_name	data_type	comment
s_lenght            	float               	                    
s_width             	float               	                    
p_length            	float               	                    
p_width             	float               	                    
clase               	string              	                    
Time taken: 0.223 seconds, Fetched: 5 row(s)
```

Importar el fichero “iris_completo.txt” al local file system del cluster en la carpeta /home/cloudera/ejercicios/ejercicios_HIVE.

```bash
[cloudera@quickstart ~]$ hadoop fs -mkdir /user/cloudera/hive
[cloudera@quickstart ~]$ hadoop fs -put /home/cloudera/ejercicios/ejercicios_HIVE/iris_completo.txt /user/cloudera/hive
```

Importa el fichero en la tabla iris que acabamos de crear desde HDFS.

```
hive> LOAD DATA INPATH '/user/cloudera/hive/iris_completo.txt' INTO TABLE iris;
Loading data to table cursohivedb.iris
Table cursohivedb.iris stats: [numFiles=1, totalSize=4551]
OK
Time taken: 0.845 seconds
```

Comprobar que la table tiene datos.

```
hive> SELECT * FROM iris;
OK
iris.s_lenght	iris.s_width	iris.p_length	iris.p_width	iris.clase
5.1	3.5	1.4	0.2	Iris-setosa
4.9	3.0	1.4	0.2	Iris-setosa
4.7	3.2	1.3	0.2	Iris-setosa
4.6	3.1	1.5	0.2	Iris-setosa
5.0	3.6	1.4	0.2	Iris-setosa
5.4	3.9	1.7	0.4	Iris-setosa
4.6	3.4	1.4	0.3	Iris-setosa
5.0	3.4	1.5	0.2	Iris-setosa
4.4	2.9	1.4	0.2	Iris-setosa
4.9	3.1	1.5	0.1	Iris-setosa
Time taken: 0.334 seconds, Fetched: 151 row(s)
```

Mostrar las 5 primeras filas de la tabla iris.

```
hive> SELECT * FROM iris limit 5;
OK
iris.s_lenght	iris.s_width	iris.p_length	iris.p_width	iris.clase
5.1	3.5	1.4	0.2	Iris-setosa
4.9	3.0	1.4	0.2	Iris-setosa
4.7	3.2	1.3	0.2	Iris-setosa
4.6	3.1	1.5	0.2	Iris-setosa
5.0	3.6	1.4	0.2	Iris-setosa
Time taken: 0.075 seconds, Fetched: 5 row(s)
```

Mostrar solo aquellas filas cuyo s_length sea mayor que 5. Observad que se ejecuta un MapReduce y que el tiempo de ejecución es un poco mayor.

```
hive> SELECT * FROM iris WHERE s_lenght > 5;
OK
iris.s_lenght	iris.s_width	iris.p_length	iris.p_width	iris.clase
5.1	3.5	1.4	0.2	Iris-setosa
5.4	3.9	1.7	0.4	Iris-setosa
5.4	3.7	1.5	0.2	Iris-setosa
5.8	4.0	1.2	0.2	Iris-setosa
5.7	4.4	1.5	0.4	Iris-setosa
5.4	3.9	1.3	0.4	Iris-setosa
5.1	3.5	1.4	0.3	Iris-setosa
5.7	3.8	1.7	0.3	Iris-setosa
5.1	3.8	1.5	0.3	Iris-setosa
5.4	3.4	1.7	0.2	Iris-setosa
Time taken: 0.157 seconds, Fetched: 118 row(s)
```

Seleccionar la media de s_width agrupados por clase. Observad que ahora el tiempo de ejecución aumenta considerablemente.

```
hive> SELECT avg(s_width) FROM iris GROUP BY clase;
Query ID = cloudera_20220325132121_6f4fecb5-004a-439f-a69f-fa3cdcc376f5
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0001, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0001/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0001
Interrupting... Be patient, this might take some time.
Press Ctrl+C again to kill JVM
killing job with: job_1648208237919_0001
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 0
2022-03-25 13:22:11,214 Stage-1 map = 0%,  reduce = 0%
Ended Job = job_1648208237919_0001 with errors
Error during job, obtaining debugging information...
Job Tracking URL: http://quickstart.cloudera:8088/cluster/app/application_1648208237919_0001
FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 FAIL
Total MapReduce CPU Time Spent: 0 msec
hive> SELECT avg(s_width) FROM iris GROUP BY clase;
Query ID = cloudera_20220325132222_f5c0ef66-94e1-4564-abaa-d0abd1c05fb7
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0002, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0002/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0002
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 13:22:38,172 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:22:46,870 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.35 sec
2022-03-25 13:22:54,312 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.0 sec
MapReduce Total cumulative CPU time: 3 seconds 0 msec
Ended Job = job_1648208237919_0002
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.0 sec   HDFS Read: 13319 HDFS Write: 57 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 0 msec
OK
_c0
NULL
3.41800000667572
2.770000009536743
2.9739999914169313
Time taken: 27.967 seconds, Fetched: 4 row(s)
```

Pregunta: vemos que aparece un valor NULL como resultado en la query anterior. ¿Por qué? ¿Cómo los eliminarías?.

```
hive> SELECT avg(s_width) FROM iris GROUP BY clase WHERE s_width IS NOT NULL;
FAILED: ParseException line 1:45 missing EOF at 'WHERE' near 'clase'
hive> SELECT avg(s_width) FROM iris WHERE s_width IS NOT NULL GROUP BY clase;
Query ID = cloudera_20220325133333_88fbde20-f4b7-4284-87a5-2a946cff14d2
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0005, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0005/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0005
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 13:33:59,186 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:34:07,812 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.73 sec
2022-03-25 13:34:17,453 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.48 sec
MapReduce Total cumulative CPU time: 3 seconds 480 msec
Ended Job = job_1648208237919_0005
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.48 sec   HDFS Read: 13664 HDFS Write: 53 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 480 msec
OK
_c0
3.41800000667572
2.770000009536743
2.978431365069221
Time taken: 27.844 seconds, Fetched: 3 row(s)
```

Insertar en la tabla la siguiente fila (1.0,3.2,4.3,5.7,"Iris-virginica").

```
hive> INSERT INTO iris VALUES (1.0, 3.2, 4.3, 5.7, 'Iris-virginica');
Query ID = cloudera_20220325132626_8bbdb0b4-e332-4b8f-b24a-1f4ff0faae5d
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1648208237919_0003, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0003/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0003
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-03-25 13:26:52,783 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:27:00,248 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.06 sec
MapReduce Total cumulative CPU time: 2 seconds 60 msec
Ended Job = job_1648208237919_0003
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to: hdfs://quickstart.cloudera:8020/user/hive/warehouse/cursohivedb.db/iris/.hive-staging_hive_2022-03-25_13-26-44_827_3379051755677496505-1/-ext-10000
Loading data to table cursohivedb.iris
Table cursohivedb.iris stats: [numFiles=2, numRows=1, totalSize=4582, rawDataSize=30]
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 2.06 sec   HDFS Read: 4708 HDFS Write: 103 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 60 msec
OK
_col0	_col1	_col2	_col3	_col4
Time taken: 17.139 seconds
```

Contar el número de ocurrencias de cada clase.

```
hive> SELECT COUNT(*), clase FROM iris GROUP BY clase;
Query ID = cloudera_20220325132828_836668f3-0f16-4772-9a64-269878d18bab
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0004, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0004/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0004
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 13:28:26,425 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:28:33,828 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.33 sec
2022-03-25 13:28:42,290 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.03 sec
MapReduce Total cumulative CPU time: 3 seconds 30 msec
Ended Job = job_1648208237919_0004
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.03 sec   HDFS Read: 13006 HDFS Write: 57 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 30 msec
OK
_c0	clase
1	NULL
50	Iris-setosa
50	Iris-versicolor
51	Iris-virginica
Time taken: 26.509 seconds, Fetched: 4 row(s)
```

Seleccionar las clases que tengan más de 45 ocurrencias.

```
hive> SELECT COUNT(*), clase FROM iris  GROUP BY clase HAVING COUNT(*) > 45;
Query ID = cloudera_20220325133838_aff10b77-b436-4e5e-966b-76f7d5c096d3
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0006, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0006/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 13:39:00,571 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:39:09,263 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.47 sec
2022-03-25 13:39:17,662 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.46 sec
MapReduce Total cumulative CPU time: 3 seconds 460 msec
Ended Job = job_1648208237919_0006
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.46 sec   HDFS Read: 13504 HDFS Write: 52 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 460 msec
OK
_c0	clase
50	Iris-setosa
50	Iris-versicolor
51	Iris-virginica
Time taken: 25.278 seconds, Fetched: 3 row(s)
```

Utilizando la función LEAD, ejecutar una query que devuelva la clase, p_length y el LEAD de p_length con Offset = 1 y Default_Value = 0, particionado por clase y ordenado por p_length.

```
hive> SELECT clase, p_length, LEAD(p_length, 1, 0) OVER (PARTITION BY clase ORDER BY p_length) as Lead FROM iris;
Query ID = cloudera_20220325135050_dba45fd8-1765-4923-8694-bf0579f7e43e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0007, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0007/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0007
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 13:50:19,793 Stage-1 map = 0%,  reduce = 0%
2022-03-25 13:50:27,251 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.53 sec
2022-03-25 13:50:35,678 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.76 sec
MapReduce Total cumulative CPU time: 3 seconds 760 msec
Ended Job = job_1648208237919_0007
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.76 sec   HDFS Read: 14178 HDFS Write: 3383 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 760 msec
OK
clase	p_length	lead
NULL	NULL	0.0
Iris-setosa	1.0	1.1
Iris-setosa	1.1	1.2
Iris-setosa	1.2	1.2
Iris-setosa	1.2	1.3
Iris-setosa	1.3	1.3
Iris-setosa	1.3	1.3
Iris-setosa	1.3	1.3
Iris-setosa	1.3	1.3
Iris-setosa	1.3	1.3
Time taken: 24.119 seconds, Fetched: 152 row(s)
```

Utilizando funciones de ventanas, seleccionar la clase, p_length, s_length, p_width, el número de valores distintos de p_length en todo el dataset, el valor máximo de s_length por clase y la media de p_width por clase, ordenado por clase y s_length de manera descendente.

```
hive> SELECT clase, s_lenght, p_length, p_width, COUNT(p_length) OVER (PARTITION BY p_length) AS valores, max(s_lenght) OVER (PARTITION BY clase) as maximo, AVG(p_width) OVER (PARTITION BY clase) as media FROM iris ORDER BY clase,s_lenght desc;
Query ID = cloudera_20220325140101_50be18ab-638e-4f14-84ee-a742486e563b
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0008, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0008/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0008
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-03-25 14:01:56,918 Stage-1 map = 0%,  reduce = 0%
2022-03-25 14:02:04,416 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.46 sec
2022-03-25 14:02:12,886 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 3.72 sec
MapReduce Total cumulative CPU time: 3 seconds 720 msec
Ended Job = job_1648208237919_0008
Launching Job 2 out of 3
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0009, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0009/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0009
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2022-03-25 14:02:21,648 Stage-2 map = 0%,  reduce = 0%
2022-03-25 14:02:29,085 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.16 sec
2022-03-25 14:02:36,562 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.36 sec
MapReduce Total cumulative CPU time: 3 seconds 360 msec
Ended Job = job_1648208237919_0009
Launching Job 3 out of 3
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1648208237919_0010, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1648208237919_0010/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1648208237919_0010
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2022-03-25 14:02:45,767 Stage-3 map = 0%,  reduce = 0%
2022-03-25 14:02:53,112 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 1.13 sec
2022-03-25 14:03:01,546 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 2.79 sec
MapReduce Total cumulative CPU time: 2 seconds 790 msec
Ended Job = job_1648208237919_0010
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.72 sec   HDFS Read: 13557 HDFS Write: 6869 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.36 sec   HDFS Read: 15248 HDFS Write: 8701 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 2.79 sec   HDFS Read: 15350 HDFS Write: 7797 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 870 msec
OK
clase	s_lenght	p_length	p_width	valores	maximo	media
NULL	NULL	NULL	NULL	0	NULL	NULL
Iris-setosa	5.8	1.2	0.2	2	5.8	0.24400000482797624
Iris-setosa	5.7	1.7	0.3	4	5.8	0.24400000482797624
Iris-setosa	5.7	1.5	0.4	14	5.8	0.24400000482797624
Iris-setosa	5.5	1.4	0.2	12	5.8	0.24400000482797624
Iris-setosa	5.5	1.3	0.2	7	5.8	0.24400000482797624
Iris-setosa	5.4	1.5	0.4	14	5.8	0.24400000482797624
Iris-setosa	5.4	1.7	0.4	4	5.8	0.24400000482797624
Iris-setosa	5.4	1.5	0.2	14	5.8	0.24400000482797624
Iris-setosa	5.4	1.7	0.2	4	5.8	0.24400000482797624
Time taken: 73.505 seconds, Fetched: 152 row(s)
```