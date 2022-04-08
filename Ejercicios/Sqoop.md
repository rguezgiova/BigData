### Creación de la tabla en MySQL

Probamos la conexión con MySQL.

```
[cloudera@quickstart ~]$ mysql -u root -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 17
Server version: 5.1.73 Source distribution

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

Vemos las BBDD que contiene.

```
mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| cm                 |
| firehose           |
| hue                |
| metastore          |
| mysql              |
| nav                |
| navms              |
| oozie              |
| retail_db          |
| rman               |
| sentry             |
+--------------------+
12 rows in set (0.02 sec)
```

Creamos en MySQL la table que queremos importar en HIVE.

```
mysql> CREATE DATABASE pruebadb;
Query OK, 1 row affected (0.06 sec)
```

Creamos una tabla con datos que luego importaremos a HIVE mediante Sqoop.

```
mysql> USE pruebadb;
Database changed
```

```
mysql> CREATE TABLE personas (nombre varchar(30), edad int);
Query OK, 0 rows affected (0.04 sec)
```

Comprobamos que se ha creado.

```
mysql> SHOW TABLES;
+--------------------+
| Tables_in_pruebadb |
+--------------------+
| personas           |
+--------------------+
1 row in set (0.02 sec)
```

```
mysql> DESCRIBE personas;
+--------+-------------+------+-----+---------+-------+
| Field  | Type        | Null | Key | Default | Extra |
+--------+-------------+------+-----+---------+-------+
| nombre | varchar(30) | YES  |     | NULL    |       |
| edad   | int(11)     | YES  |     | NULL    |       |
+--------+-------------+------+-----+---------+-------+
2 rows in set (0.02 sec)
```

Importamos algunas filas.

```
mysql> INSERT INTO personas VALUES ("Alberto", 22);
Query OK, 1 row affected (0.03 sec)

mysql> INSERT INTO personas VALUES ("Luis", 23);
Query OK, 1 row affected (0.02 sec)

mysql> INSERT INTO personas VALUES ("Pablo", 24);
Query OK, 1 row affected (0.05 sec)

mysql> INSERT INTO personas VALUES ("Carlos", 25);
Query OK, 1 row affected (0.00 sec)
```

Comprobamos que los datos se han insertado en la tabla.

```
mysql> SELECT * FROM personas;
+---------+------+
| nombre  | edad |
+---------+------+
| Alberto |   22 |
| Luis    |   23 |
| Pablo   |   24 |
| Carlos  |   25 |
+---------+------+
4 rows in set (0.02 sec)
```

### Creación de la tabla en HIVE.

Creamos la tabla en HIVE donde se importarán los datos que acabamos de crear.

Accedemos a hive.

```
[cloudera@quickstart ~]$ hive
Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> 
```

Creamos una base de datos para esta prueba y accedemos a ella.

```
hive> CREATE DATABASE prueba_sqoop_hive;
OK
Time taken: 3.127 seconds
```

```
hive> USE prueba_sqoop_hive;
OK
Time taken: 0.132 seconds
```

Creamos la estructura de la table que contendrá los datos importados desde MySQL con SQOOP.

```
hive> CREATE TABLE personas (nombre string, edad int) ROW FORMAT DELIMITED STORED AS TEXTFILE;
OK
Time taken: 0.428 seconds
```

Comprobamos que se ha creado con éxito.

```
hive> SHOW TABLES;
OK
personas
Time taken: 0.098 seconds, Fetched: 1 row(s)
```

### Importamos la tabla con SQOOP

Dado que la "BBDD" Accumulo no está configurada, abrimos un Shell y ejecutamos los siguientes comandos para evitar warnings molestos.

```bash
[cloudera@quickstart ~]$ sudo mkdir /var/lib/accumulo
[cloudera@quickstart ~]$ ACCUMULO_HOME='/var/lib/accumulo'
[cloudera@quickstart ~]$ export ACCUMULO_HOME
```

En un Shell escribimos lo siguiente para ver que SQOOP está conectado con nuestro MySQL:

```bash
[cloudera@quickstart ~]$ sqoop list-databases --connect jdbc:mysql://localhost --username root --password cloudera
22/03/28 11:22:11 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
22/03/28 11:22:11 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/03/28 11:22:12 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
information_schema
cm
firehose
hue
metastore
mysql
nav
navms
oozie
pruebadb
retail_db
rman
sentry
```

Ahora listamos la tabla "personas" de la BBDD "pruebadb" que hemos creado en MySQL.

```bash
[cloudera@quickstart ~]$ sqoop list-tables --connect jdbc:mysql://localhost/pruebadb --username root --password cloudera
22/03/28 11:23:30 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
22/03/28 11:23:30 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/03/28 11:23:31 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
personas
```

Usando los argumentos de importación hive mostrados en las slides del curso, importar la tabla creada en Mysql en la estructura creada en HIVE.

```
[cloudera@quickstart ~]$ sqoop import --connect jdbc:mysql://localhost/pruebadb --table personas --username root --password cloudera -m 1 --hive-import --hive-overwrite --hive-table prueba_sqoop_hive.personas_hive
22/03/28 11:27:45 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
22/03/28 11:27:45 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/03/28 11:27:45 INFO tool.BaseSqoopTool: Using Hive-specific delimiters for output. You can override
22/03/28 11:27:45 INFO tool.BaseSqoopTool: delimiters with --fields-terminated-by, etc.
22/03/28 11:27:45 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
22/03/28 11:27:45 INFO tool.CodeGenTool: Beginning code generation
22/03/28 11:27:46 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `personas` AS t LIMIT 1
22/03/28 11:27:46 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `personas` AS t LIMIT 1
22/03/28 11:27:46 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/lib/hadoop-mapreduce
Note: /tmp/sqoop-cloudera/compile/91d3fc7f61d67e18a152f875c4386fde/personas.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/03/28 11:27:49 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-cloudera/compile/91d3fc7f61d67e18a152f875c4386fde/personas.jar
22/03/28 11:27:49 WARN manager.MySQLManager: It looks like you are importing from mysql.
22/03/28 11:27:49 WARN manager.MySQLManager: This transfer can be faster! Use the --direct
22/03/28 11:27:49 WARN manager.MySQLManager: option to exercise a MySQL-specific fast path.
22/03/28 11:27:49 INFO manager.MySQLManager: Setting zero DATETIME behavior to convertToNull (mysql)
22/03/28 11:27:49 INFO mapreduce.ImportJobBase: Beginning import of personas
22/03/28 11:27:49 INFO Configuration.deprecation: mapred.job.tracker is deprecated. Instead, use mapreduce.jobtracker.address
22/03/28 11:27:50 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/03/28 11:27:51 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/03/28 11:27:51 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/03/28 11:27:59 INFO db.DBInputFormat: Using read commited transaction isolation
22/03/28 11:28:00 INFO mapreduce.JobSubmitter: number of splits:1
22/03/28 11:28:00 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1648453540520_0001
22/03/28 11:28:01 INFO impl.YarnClientImpl: Submitted application application_1648453540520_0001
22/03/28 11:28:01 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1648453540520_0001/
22/03/28 11:28:01 INFO mapreduce.Job: Running job: job_1648453540520_0001
22/03/28 11:28:15 INFO mapreduce.Job: Job job_1648453540520_0001 running in uber mode : false
22/03/28 11:28:15 INFO mapreduce.Job:  map 0% reduce 0%
22/03/28 11:28:23 INFO mapreduce.Job:  map 100% reduce 0%
22/03/28 11:28:24 INFO mapreduce.Job: Job job_1648453540520_0001 completed successfully
22/03/28 11:28:25 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=171135
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=87
		HDFS: Number of bytes written=38
		HDFS: Number of read operations=4
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Other local map tasks=1
		Total time spent by all maps in occupied slots (ms)=6868
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=6868
		Total vcore-milliseconds taken by all map tasks=6868
		Total megabyte-milliseconds taken by all map tasks=7032832
	Map-Reduce Framework
		Map input records=4
		Map output records=4
		Input split bytes=87
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=31
		CPU time spent (ms)=950
		Physical memory (bytes) snapshot=179671040
		Virtual memory (bytes) snapshot=1569181696
		Total committed heap usage (bytes)=173015040
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=38
22/03/28 11:28:25 INFO mapreduce.ImportJobBase: Transferred 38 bytes in 33,5505 seconds (1,1326 bytes/sec)
22/03/28 11:28:25 INFO mapreduce.ImportJobBase: Retrieved 4 records.
22/03/28 11:28:25 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `personas` AS t LIMIT 1
22/03/28 11:28:25 INFO hive.HiveImport: Loading uploaded data into Hive

Logging initialized using configuration in jar:file:/usr/lib/hive/lib/hive-common-1.1.0-cdh5.13.0.jar!/hive-log4j.properties
OK
Time taken: 2.473 seconds
Loading data to table prueba_sqoop_hive.personas_hive
chgrp: changing ownership of 'hdfs://quickstart.cloudera:8020/user/hive/warehouse/prueba_sqoop_hive.db/personas_hive': User does not belong to supergroup
Table prueba_sqoop_hive.personas_hive stats: [numFiles=1, numRows=0, totalSize=38, rawDataSize=0]
OK
Time taken: 1.38 seconds
```

