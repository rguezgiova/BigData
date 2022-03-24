Crea un directorio local de trabajo.

a. Por ejemplo /home/cloudera/ejercicios.

```bash
[cloudera@quickstart ejercicios]$ mkdir ejercicios
```

b. Copia el archivo “shakespeare.tar.gz” en ese directorio.

```bash
[cloudera@quickstart ejercicios]$ cp /mnt/hgfs/datasets/shakespeare.tar.gz ejercicios/
```

c. Descomprime el archivo.

```bash
[cloudera@quickstart ejercicios]$ tar zxvf shakespeare.tar.gz 
shakespeare/
shakespeare/comedies
shakespeare/glossary
shakespeare/histories
shakespeare/poems
shakespeare/tragedies
```

d. Copia la carpeta que acabas de descomprimir en HDFS en la ruta /user/cloudera/shakespeare. Tu home a partir de ahora será /user/cloudera.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -put shakespeare /user/cloudera/shakespeare
```

Lista el contenido de tu home en HDFS para comprobar que se ha copiado la carpeta shakespeare.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -ls /user/cloudera
Found 1 items
drwxr-xr-x   - cloudera cloudera          0 2022-03-24 18:21 /user/cloudera/shakespeare
```

Observa el contenido de la carpeta shakespeare en HDFS.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -ls /user/cloudera/shakespeare
Found 5 items
-rw-r--r--   1 cloudera cloudera    1784616 2022-03-24 18:21 /user/cloudera/shakespeare/comedies
-rw-r--r--   1 cloudera cloudera      58976 2022-03-24 18:21 /user/cloudera/shakespeare/glossary
-rw-r--r--   1 cloudera cloudera    1479035 2022-03-24 18:21 /user/cloudera/shakespeare/histories
-rw-r--r--   1 cloudera cloudera     268140 2022-03-24 18:21 /user/cloudera/shakespeare/poems
-rw-r--r--   1 cloudera cloudera    1752440 2022-03-24 18:21 /user/cloudera/shakespeare/tragedies
```

Borra la sub carpeta “glossary” de la carpeta shakespeare en HDFS.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -rm /user/cloudera/shakespeare/glossary
Deleted /user/cloudera/shakespeare/glossary
```

Comprueba que se ha borrado.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -ls /user/cloudera/shakespeareFound 4 items
-rw-r--r--   1 cloudera cloudera    1784616 2022-03-24 18:21 /user/cloudera/shakespeare/comedies
-rw-r--r--   1 cloudera cloudera    1479035 2022-03-24 18:21 /user/cloudera/shakespeare/histories
-rw-r--r--   1 cloudera cloudera     268140 2022-03-24 18:21 /user/cloudera/shakespeare/poems
-rw-r--r--   1 cloudera cloudera    1752440 2022-03-24 18:21 /user/cloudera/shakespeare/tragedies
```

Lista las primeras 50 últimas líneas de la sub carpeta “histories”.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -cat /user/cloudera/shakespeare/histories | tail -n 50
RICHMOND	God and your arms be praised, victorious friends,
	The day is ours, the bloody dog is dead.

DERBY	Courageous Richmond, well hast thou acquit thee.
	Lo, here, this long-usurped royalty
	From the dead temples of this bloody wretch
	Have I pluck'd off, to grace thy brows withal:
	Wear it, enjoy it, and make much of it.

RICHMOND	Great God of heaven, say Amen to all!
	But, tell me, is young George Stanley living?

DERBY	He is, my lord, and safe in Leicester town;
	Whither, if it please you, we may now withdraw us.

RICHMOND	What men of name are slain on either side?

DERBY	John Duke of Norfolk, Walter Lord Ferrers,
	Sir Robert Brakenbury, and Sir William Brandon.

RICHMOND	Inter their bodies as becomes their births:
	Proclaim a pardon to the soldiers fled
	That in submission will return to us:
	And then, as we have ta'en the sacrament,
	We will unite the white rose and the red:
	Smile heaven upon this fair conjunction,
	That long have frown'd upon their enmity!
	What traitor hears me, and says not amen?
	England hath long been mad, and scarr'd herself;
	The brother blindly shed the brother's blood,
	The father rashly slaughter'd his own son,
	The son, compell'd, been butcher to the sire:
	All this divided York and Lancaster,
	Divided in their dire division,
	O, now, let Richmond and Elizabeth,
	The true succeeders of each royal house,
	By God's fair ordinance conjoin together!
	And let their heirs, God, if thy will be so.
	Enrich the time to come with smooth-faced peace,
	With smiling plenty and fair prosperous days!
	Abate the edge of traitors, gracious Lord,
	That would reduce these bloody days again,
	And make poor England weep in streams of blood!
	Let them not live to taste this land's increase
	That would with treason wound this fair land's peace!
	Now civil wounds are stopp'd, peace lives again:
	That she may long live here, God say amen!

	[Exeunt]
	[END]
```

Copia al Sistema de ficheros local de tu MV el fichero “poems” en la ruta /home/cloudera/ejercicios/shakespeare/shakepoems.txt.

```bash
[cloudera@quickstart ejercicios]$ hadoop fs -get /user/cloudera/shakespeare/poems /home/cloudera/ejercicios/shakespeare/shakepoems.txt
```

Muestra las últimas líneas de shakepoems.txt copiado en tu local por pantalla.

```bash
[cloudera@quickstart ejercicios]$ tail 5 shakespeare/shakepoems.txt 
==> shakespeare/shakepoems.txt <==
O, that false fire which in his cheek so glow'd,
O, that forced thunder from his heart did fly,
O, that sad breath his spongy lungs bestow'd,
O, all that borrow'd motion seeming owed,
Would yet again betray the fore-betray'd,
And new pervert a reconciled maid!'



	[END]
```

Copiar en la ruta “/home/cloudera/ejercicios” la carpeta “wordcount” y su contenido.

```bash
[cloudera@quickstart ejercicios]$ cp -r /mnt/hgfs/datasets/wordcount/ .
```

Comprobar que se han copiado correctamente.

```bash
[cloudera@quickstart ejercicios]$ ll
total 2044
drwxr-xr-x 2 cloudera cloudera    4096 mar 24 18:29 shakespeare
-rwxrwx--- 1 cloudera cloudera 2080857 mar 24 18:17 shakespeare.tar.gz
drwxrwx--- 2 cloudera cloudera    4096 mar 24 18:34 wordcount
```

La carpeta wordcount, como hemos visto, ya contiene los javas compilados y el jar creado, por lo que solo tenemos que ejecutar el submit del job hadoop usando nuestro fichero JAR para contar las ocurrencias de palabras contenidas en nuestra carpeta “shakespeare”. Nuestro jar contiene las clases java compiladas dentro de un paquete llamado “solutions”, por eso se le llama de este modo.

```bash
[cloudera@quickstart wordcount]$ hadoop jar wc.jar solution.WordCount shakespeare /user/cloudera/wordcounts
22/03/24 18:37:11 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/03/24 18:37:11 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
22/03/24 18:37:12 INFO input.FileInputFormat: Total input paths to process : 4
22/03/24 18:37:12 INFO mapreduce.JobSubmitter: number of splits:4
22/03/24 18:37:12 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1648138848832_0001
22/03/24 18:37:13 INFO impl.YarnClientImpl: Submitted application application_1648138848832_0001
22/03/24 18:37:13 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1648138848832_0001/
22/03/24 18:37:13 INFO mapreduce.Job: Running job: job_1648138848832_0001
22/03/24 18:37:23 INFO mapreduce.Job: Job job_1648138848832_0001 running in uber mode : false
22/03/24 18:37:23 INFO mapreduce.Job:  map 0% reduce 0%
22/03/24 18:37:40 INFO mapreduce.Job:  map 25% reduce 0%
22/03/24 18:37:43 INFO mapreduce.Job:  map 50% reduce 0%
22/03/24 18:37:49 INFO mapreduce.Job:  map 75% reduce 0%
22/03/24 18:37:50 INFO mapreduce.Job:  map 100% reduce 0%
22/03/24 18:37:55 INFO mapreduce.Job:  map 100% reduce 100%
22/03/24 18:37:56 INFO mapreduce.Job: Job job_1648138848832_0001 completed successfully
22/03/24 18:37:56 INFO mapreduce.Job: Counters: 50
	File System Counters
		FILE: Number of bytes read=10713042
		FILE: Number of bytes written=22142092
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=5284754
		HDFS: Number of bytes written=299379
		HDFS: Number of read operations=15
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Killed map tasks=1
		Launched map tasks=4
		Launched reduce tasks=1
		Data-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=78739
		Total time spent by all reduces in occupied slots (ms)=10612
		Total time spent by all map tasks (ms)=78739
		Total time spent by all reduce tasks (ms)=10612
		Total vcore-milliseconds taken by all map tasks=78739
		Total vcore-milliseconds taken by all reduce tasks=10612
		Total megabyte-milliseconds taken by all map tasks=80628736
		Total megabyte-milliseconds taken by all reduce tasks=10866688
	Map-Reduce Framework
		Map input records=173126
		Map output records=964453
		Map output bytes=8784130
		Map output materialized bytes=10713060
		Input split bytes=523
		Combine input records=0
		Combine output records=0
		Reduce input groups=29183
		Reduce shuffle bytes=10713060
		Reduce input records=964453
		Reduce output records=29183
		Spilled Records=1928906
		Shuffled Maps =4
		Failed Shuffles=0
		Merged Map outputs=4
		GC time elapsed (ms)=1687
		CPU time spent (ms)=13710
		Physical memory (bytes) snapshot=1436164096
		Virtual memory (bytes) snapshot=7800844288
		Total committed heap usage (bytes)=1278738432
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=5284231
	File Output Format Counters 
		Bytes Written=299379

```

Una vez ejecutado, probamos a ejecutarlo nuevamente.

```bash
[cloudera@quickstart wordcount]$ hadoop jar wc.jar solution.WordCount shakespeare /user/cloudera/wordcounts
22/03/24 18:41:09 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/03/24 18:41:09 WARN security.UserGroupInformation: PriviledgedActionException as:cloudera (auth:SIMPLE) cause:org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://quickstart.cloudera:8020/user/cloudera/wordcounts already exists
Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://quickstart.cloudera:8020/user/cloudera/wordcounts already exists
	at org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:146)
	at org.apache.hadoop.mapreduce.JobSubmitter.checkSpecs(JobSubmitter.java:270)
	at org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal(JobSubmitter.java:143)
	at org.apache.hadoop.mapreduce.Job$10.run(Job.java:1307)
	at org.apache.hadoop.mapreduce.Job$10.run(Job.java:1304)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:415)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1917)
	at org.apache.hadoop.mapreduce.Job.submit(Job.java:1304)
	at org.apache.hadoop.mapreduce.Job.waitForCompletion(Job.java:1325)
	at solution.WordCount.main(WordCount.java:98)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.apache.hadoop.util.RunJar.run(RunJar.java:221)
	at org.apache.hadoop.util.RunJar.main(RunJar.java:136)
```

Comprobamos el resultado de nuestro MapReduce.

```bash
[cloudera@quickstart wordcount]$ hadoop fs -ls /user/cloudera/wordcounts
Found 2 items
-rw-r--r--   1 cloudera cloudera          0 2022-03-24 18:37 /user/cloudera/wordcounts/_SUCCESS
-rw-r--r--   1 cloudera cloudera     299379 2022-03-24 18:37 /user/cloudera/wordcounts/part-r-00000
```

Observamos el contenido del fichero.

```bash
[cloudera@quickstart wordcount]$ hadoop fs -cat /user/cloudera/wordcounts/part-r-00000 | less
```

Volvemos a ejecutar el job de nuevo.

```bash
[cloudera@quickstart wordcount]$ hadoop jar wc.jar solution.WordCount shakespeare/poems /user/cloudera/pwords
22/03/24 18:44:55 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/03/24 18:44:55 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
22/03/24 18:44:56 INFO input.FileInputFormat: Total input paths to process : 1
22/03/24 18:44:57 INFO mapreduce.JobSubmitter: number of splits:1
22/03/24 18:44:57 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1648138848832_0002
22/03/24 18:44:57 INFO impl.YarnClientImpl: Submitted application application_1648138848832_0002
22/03/24 18:44:57 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1648138848832_0002/
22/03/24 18:44:57 INFO mapreduce.Job: Running job: job_1648138848832_0002
22/03/24 18:45:04 INFO mapreduce.Job: Job job_1648138848832_0002 running in uber mode : false
22/03/24 18:45:04 INFO mapreduce.Job:  map 0% reduce 0%
22/03/24 18:45:12 INFO mapreduce.Job:  map 100% reduce 0%
22/03/24 18:45:20 INFO mapreduce.Job:  map 100% reduce 100%
22/03/24 18:45:21 INFO mapreduce.Job: Job job_1648138848832_0002 completed successfully
22/03/24 18:45:21 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=558628
		FILE: Number of bytes written=1403623
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=268268
		HDFS: Number of bytes written=67271
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=4890
		Total time spent by all reduces in occupied slots (ms)=4949
		Total time spent by all map tasks (ms)=4890
		Total time spent by all reduce tasks (ms)=4949
		Total vcore-milliseconds taken by all map tasks=4890
		Total vcore-milliseconds taken by all reduce tasks=4949
		Total megabyte-milliseconds taken by all map tasks=5007360
		Total megabyte-milliseconds taken by all reduce tasks=5067776
	Map-Reduce Framework
		Map input records=7308
		Map output records=50212
		Map output bytes=458198
		Map output materialized bytes=558628
		Input split bytes=128
		Combine input records=0
		Combine output records=0
		Reduce input groups=7193
		Reduce shuffle bytes=558628
		Reduce input records=50212
		Reduce output records=7193
		Spilled Records=100424
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=250
		CPU time spent (ms)=4240
		Physical memory (bytes) snapshot=657190912
		Virtual memory (bytes) snapshot=3111821312
		Total committed heap usage (bytes)=555220992
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=268140
	File Output Format Counters 
		Bytes Written=67271
```

Borramos la salida producida por nuestros jobs.

```bash
[cloudera@quickstart wordcount]$ hadoop fs -rm  -r /user/cloudera/wordcounts /user/cloudera/pwords
Deleted /user/cloudera/pwords
```

Ejecutamos nuevamente nuestro job.

```bash
[cloudera@quickstart wordcount]$ hadoop jar wc.jar solution.WordCount shakespeare /user/cloudera/count2
22/03/24 18:48:05 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/03/24 18:48:05 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
22/03/24 18:48:05 INFO input.FileInputFormat: Total input paths to process : 4
22/03/24 18:48:06 INFO mapreduce.JobSubmitter: number of splits:4
22/03/24 18:48:06 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1648138848832_0003
22/03/24 18:48:06 INFO impl.YarnClientImpl: Submitted application application_1648138848832_0003
22/03/24 18:48:06 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1648138848832_0003/
22/03/24 18:48:06 INFO mapreduce.Job: Running job: job_1648138848832_0003
22/03/24 18:48:14 INFO mapreduce.Job: Job job_1648138848832_0003 running in uber mode : false
22/03/24 18:48:14 INFO mapreduce.Job:  map 0% reduce 0%
22/03/24 18:48:34 INFO mapreduce.Job:  map 25% reduce 0%
22/03/24 18:48:39 INFO mapreduce.Job:  map 50% reduce 0%
22/03/24 18:48:41 INFO mapreduce.Job:  map 100% reduce 0%
22/03/24 18:48:47 INFO mapreduce.Job:  map 100% reduce 100%
22/03/24 18:48:48 INFO mapreduce.Job: Job job_1648138848832_0003 completed successfully
22/03/24 18:48:48 INFO mapreduce.Job: Counters: 50
	File System Counters
		FILE: Number of bytes read=10713042
		FILE: Number of bytes written=22142072
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=5284754
		HDFS: Number of bytes written=299379
		HDFS: Number of read operations=15
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Killed map tasks=1
		Launched map tasks=4
		Launched reduce tasks=1
		Data-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=84472
		Total time spent by all reduces in occupied slots (ms)=10570
		Total time spent by all map tasks (ms)=84472
		Total time spent by all reduce tasks (ms)=10570
		Total vcore-milliseconds taken by all map tasks=84472
		Total vcore-milliseconds taken by all reduce tasks=10570
		Total megabyte-milliseconds taken by all map tasks=86499328
		Total megabyte-milliseconds taken by all reduce tasks=10823680
	Map-Reduce Framework
		Map input records=173126
		Map output records=964453
		Map output bytes=8784130
		Map output materialized bytes=10713060
		Input split bytes=523
		Combine input records=0
		Combine output records=0
		Reduce input groups=29183
		Reduce shuffle bytes=10713060
		Reduce input records=964453
		Reduce output records=29183
		Spilled Records=1928906
		Shuffled Maps =4
		Failed Shuffles=0
		Merged Map outputs=4
		GC time elapsed (ms)=2145
		CPU time spent (ms)=14680
		Physical memory (bytes) snapshot=1517207552
		Virtual memory (bytes) snapshot=7859085312
		Total committed heap usage (bytes)=1307049984
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=5284231
	File Output Format Counters 
		Bytes Written=299379
```

Mientras se ejecuta, en otro terminal ejecutamos lo siguiente, para ver la lista de Jobs que se están ejecutando.

```bash
[cloudera@quickstart wordcount]$ mapred job -list
22/03/24 18:48:31 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
Total jobs:1
                  JobId	     State	     StartTime	    UserName	       Queue	  Priority	 UsedContainers	 RsvdContainers	 UsedMem	 RsvdMem NeededMem	   AM info
 job_1648138848832_0003	   RUNNING	 1648144086538	    cloudera	root.cloudera	    NORMAL	              5	              0	   6144M	      0M     6144M	http://quickstart.cloudera:8088/proxy/application_1648138848832_0003/
```

