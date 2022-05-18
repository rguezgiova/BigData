### 1. Creación de tablas en formato texto

1.1) Crear Base de datos "datos_padron" .

```sql
CREATE DATABASE IF NOT EXISTS datos_padron;
```

1.2) Crear la tabla de datos "padron_txt" con todos los campos del fichero CSV y cargar los datos mediante el comando LOAD DATA LOCAL INPATH. La tabla tendrá formato texto y tendrá como delimitador de campo el caracter ';' y los campos que en el documento original están encerrados en comillas dobles '"' no deben estar envueltos en estos caracteres en la tabla de Hive.

```sql
CREATE EXTERNAL TABLE padron_txt (
    cod_distrito int,
    desc_distrito string,
    cod_dist_barrio int,
    desc_barrio string,
    cod_barrio int,
    cod_dist_seccion int,
    cod_seccion int,
    cod_edad_int int,
    EspanolesHombres int,
    EspanolesMujeres int,
    ExtranjerosHombres int,
    ExtranjerosMujeres int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
    "separatorChar" = ';',
    "quoteChar"     = '"',
    'escapeChar'    = '\\') 
    STORED AS TEXTFILE
	TBLPROPERTIES (
    'skip.header.line.count' = '1');
```

1.3) Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la tabla resultado como "padron_txt_2".

```sql
CREATE TABLE padron_txt_2 AS
SELECT
    CAST(COD_DISTRITO AS INT),
    RTRIM(DESC_DISTRITO) DESC_DISTRITO,
    CAST(COD_DIST_BARRIO AS INT),
    RTRIM(DESC_BARRIO) DESC_BARRIO,
    CAST(COD_BARRIO AS INT),
    CAST(COD_DIST_SECCION AS INT),
    CAST(COD_SECCION AS INT),
    CAST(COD_EDAD_INT AS INT),
    CAST(CASE WHEN(LENGTH(EspanolesHombres) = 0) THEN 0 ELSE EspanolesHombres END AS INT) EspanolesHombres,
    CAST(CASE WHEN(LENGTH(EspanolesMujeres) = 0) THEN 0 ELSE EspanolesMujeres END AS INT) EspanolesMujeres,
    CAST(CASE WHEN(LENGTH(ExtranjerosHombres) = 0) THEN 0 ELSE ExtranjerosHombres END AS INT) ExtranjerosHombres,
    CAST(CASE WHEN(LENGTH(ExtranjerosMujeres) = 0) THEN 0 ELSE ExtranjerosMujeres END AS INT) ExtranjerosMujeres
FROM padron_txt;
```

1.4) Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD DATA.

La diferencia es que si se escribe **LOCAL** el fichero tiene que estar en tu máquina local y si se omite dicha palabra estará en HDFS.

1.5) En este momento te habrás dado cuenta de un aspecto importante, los datos nulos de nuestras tablas vienen representados por un espacio vacío y no por un identificador de nulos comprensible para la tabla. Esto puede ser un problema para el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva tabla utiliando sentencias case when que sustituyan espacios en blanco por 0.

```sql
CREATE TABLE padron_txt_2 AS
SELECT
    CAST(COD_DISTRITO AS INT),
    RTRIM(DESC_DISTRITO) DESC_DISTRITO,
    CAST(COD_DIST_BARRIO AS INT),
    RTRIM(DESC_BARRIO) DESC_BARRIO,
    CAST(COD_BARRIO AS INT),
    CAST(COD_DIST_SECCION AS INT),
    CAST(COD_SECCION AS INT),
    CAST(COD_EDAD_INT AS INT),
    CAST(CASE WHEN(LENGTH(EspanolesHombres) = 0) THEN 0 ELSE EspanolesHombres END AS INT) EspanolesHombres,
    CAST(CASE WHEN(LENGTH(EspanolesMujeres) = 0) THEN 0 ELSE EspanolesMujeres END AS INT) EspanolesMujeres,
    CAST(CASE WHEN(LENGTH(ExtranjerosHombres) = 0) THEN 0 ELSE ExtranjerosHombres END AS INT) ExtranjerosHombres,
    CAST(CASE WHEN(LENGTH(ExtranjerosMujeres) = 0) THEN 0 ELSE ExtranjerosMujeres END AS INT) ExtranjerosMujeres
FROM padron_txt;
```

1.6) Una manera tremendamente potente de solucionar todos los problemas previos (tanto las comillas como los campos vacíos que no son catalogados como null y los espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona OpenCSV.

```sql
CREATE EXTERNAL TABLE padron_txt_3 (
    cod_distrito int,
    desc_distrito string,
    cod_dist_barrio int,
    desc_barrio string,
    cod_barrio int,
    cod_dist_seccion int,
    cod_seccion int,
    cod_edad_int int,
    EspanolesHombres int,
    EspanolesMujeres int,
    ExtranjerosHombres int,
    ExtranjerosMujeres int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
    "input.regex" = '"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"\\s*([^\\s]*)\\s*"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"'
    )
    STORED AS TEXTFILE;
```

### 2. Investigamos el formato columnar parquet

2.1) ¿Qué es CTAS?

CTAS es una consulta CREATE TABLE AS SELECT y se utiliza para crear una tabla a partir de los resultados de una instrucción SELECT de otra consulta en un solo paso.

2.2) Crear tabla Hive "padron_parquet" (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla "padron_txt" mediante un CTAS.

```sql
CREATE TABLE padron_parquet STORED AS PARQUET AS
SELECT
    COD_DISTRITO,
    DESC_DISTRITO,
    COD_DIST_BARRIO,
    DESC_BARRIO,
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    COD_EDAD_INT,
    EspanolesHombres,
    EspanolesMujeres,
    ExtranjerosHombres,
    ExtranjerosMujeres
FROM padron_txt;
```

2.3) Crear tabla Hive "padron_parquet_2" a través de la tabla "padron_txt_2" mediante un CTAS.

```sql
CREATE TABLE padron_parquet_2 STORED AS parquet AS
SELECT 
    * 
FROM padron_txt_2;
```

2.4) Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones posteriores que la tabla padron_parquet conserve los espacios innecesarios y la tabla "padron_parquet_2" no los tenga. Dejo a tu elección cómo hacerlo.

2.5) Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos.

 * Está basado en columnas, para así ahorrar espacio y ganar velocidad de procesamiento en las sentencias.
 * Está orientado a columnas y diseñado para brindar un almacenamiento en columnas eficiente en comparación con los tipos de ficheros basados en filas.
 * Los archivos Parquet se diseñaron teniendo en cuenta estructuras de datos anidadas complejas.
 * Está diseñado para admitir esquemas de compresión y codificación muy eficientes.
 * Genera menores costes de almacenamiento para archivos de datos y maximiza la efectividad de las consultas de datos con tecnologías cloud actuales.
 * Licenciado bajo la licencia Apache y disponible para cualquier proyecto.

2.6) Comparar el tamaño de los ficheros de los datos de las tablas "padron_txt" (txt), "padron_txt_2" (txt pero no incluye los espacios innecesarios), padron_parquet y "padron_parquet_2" (alojados en hdfs cuya ruta se puede obtener de la propiedad location de cada tabla por ejemplo haciendo "show create table").

```console
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 16:13 /user/hive/warehouse/datos_padron.db/padron_parquet
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 16:17 /user/hive/warehouse/datos_padron.db/padron_parquet_2
drwxrwxrwx   - cloudera supergroup          0 2022-05-11 15:54 /user/hive/warehouse/datos_padron.db/padron_particionado
drwxrwxrwx   - cloudera supergroup          0 2022-05-09 16:47 /user/hive/warehouse/datos_padron.db/padron_txt
drwxrwxrwx   - cloudera supergroup          0 2022-05-09 16:49 /user/hive/warehouse/datos_padron.db/padron_txt2
drwxrwxrwx   - cloudera supergroup          0 2022-05-09 16:56 /user/hive/warehouse/datos_padron.db/padron_txt_2
drwxrwxrwx   - cloudera supergroup          0 2022-05-10 12:40 /user/hive/warehouse/datos_padron.db/padron_txt_3
```

### 3. Juguemos con Impala

3.1) ¿Qué es Impala?

Es un motor de consultas SQL open source para el procesamiento masivo en paralelo (MPP) de datos almacenados en clúster Apache Hadoop.

3.2) ¿En qué se diferencia de Hive?

La diferencia principal es que Hive es un software de almacenamiento de datos que se puede usar para acceder y administrar grandes conjuntos de datos distribuidos construidos en Hadoop, mientras que Impala es un motor masivo de procesamiento paralelo de SQL para administrar y analizar datos almacenados en Hadoop.

3.3) Comando INVALIDATE METADATA, ¿en qué consiste?

INVALIDATE METADATA consiste en actualizar los metadatos de toda la base de datos o una tabla, incluidos los metadatos de la tabla y los datos del archivo en la tabla. Primero borrará la memoria caché de la tabla y luego volverá a cargar todos los datos de la tienda de metadatos y la memoria caché. Es una operación bastante cara.

3.4) Hacer INVALIDATE METADATA en Impala de la base de datos datos_padron.

```sql
INVALIDATE METADATA;
```

3.5) Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.

```sql
SELECT desc_distrito, desc_barrio,
       sum(espanoleshombres) AS EspanolesHombres,
       sum(espanolesmujeres) AS EspanolesMujeres,
       sum(extranjeroshombres) AS ExtranjerosHombres,
       sum(extranjerosmujeres) AS ExtranjerosMujeres
FROM padron_txt_2 
GROUP BY desc_distrito, desc_barrio;
```

3.6) Llevar a cabo las consultas en Hive en las tablas "padron_txt_2" y "padron_parquet_2" (No deberían incluir espacios innecesarios). ¿Alguna conclusión?

Que los tiempos en lo que tardan las consultas en ejecutarse son similares.

3.7) Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Alguna conclusión?

Los tiempos de ejecución en Impala son bastante inferiores.

3.8) ¿Se percibe alguna diferencia de rendimiento entre Hive e Impala?

Impala es mucho más ágil a la hora de realizar consultas.

### 4. Sobre tablas particionadas

4.1) Crear tabla (Hive) "padron_particionado" particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.

```sql
CREATE EXTERNAL TABLE padron_particionado (
    COD_DISTRITO int,
    COD_DIST_BARRIO int,
    COD_BARRIO int,
    COD_DIST_SECCION int,
    COD_SECCION int,
    COD_EDAD_INT int,
    EspanolesHombres int,
    EspanolesMujeres int,
    ExtranjerosHombres int,
    ExtranjerosMujeres int)
    PARTITIONED BY (DESC_DISTRITO string, DESC_BARRIO string)
    STORED AS PARQUET
    LOCATION '/user/cloudera/particionado';
```

4.2) Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla "padron_parquet_2".

```sql
INSERT INTO TABLE padron_particionado PARTITION(desc_distrito, desc_barrio)
SELECT
    *
FROM datos_padron.padron_parquet_2;
```

4.3) Hacer invalidate metadata en Impala de la base de datos padron_particionado.

```sql
INVALIDATE METADATA padron_particionado;
```

4.4) Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.

```sql
SELECT DESC_DISTRITO, DESC_BARRIO, 
    SUM(EspanolesHombres) AS totalEspanolesHombres,
    SUM(EspanolesMujeres) AS totalEspanolesMujeres,
    SUM(ExtranjerosHombres) AS totalExtranjerosHombres,
    SUM(ExtranjerosMujeres) AS totalExtranjerosMujeres
FROM padron_particionado
WHERE DESC_DISTRITO IN ('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS') 
GROUP BY DESC_DISTRITO, DESC_BARRIO
ORDER BY DESC_DISTRITO;
```

4.5) Llevar a cabo la consulta en Hive en las tablas "padron_parquet" y "padron_partitionado". ¿Alguna conclusión?

En la tabla "padron_particionado" la consulta es ligeramente más rápida.

4.6) Llevar a cabo la consulta en Impala en las tablas "padron_parquet" y "padron_particionado". ¿Alguna conclusión?

Al igual que en Hive en la tabla "padron_particionado" la consulta es ligeramente más rápida.

4.7) Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas ("padron_txt_2", "padron_parquet_2" y "padron_particionado") y comparar rendimientos tanto en Hive como en Impala y sacar conclusiones.

```sql
SELECT desc_distrito, desc_barrio,
       max(espanoleshombres), min(espanolesmujeres),
       avg(extranjeroshombres), count(extranjerosmujeres)
FROM padron_txt_2
GROUP BY desc_distrito, desc_barrio;

SELECT desc_distrito, desc_barrio,
       max(espanoleshombres), min(espanolesmujeres),
       avg(extranjeroshombres), count(extranjerosmujeres)
FROM padron_parquet_2
GROUP BY desc_distrito, desc_barrio;

SELECT desc_distrito, desc_barrio,
       max(espanoleshombres), min(espanolesmujeres),
       avg(extranjeroshombres), count(extranjerosmujeres)
FROM padron_particionado
GROUP BY desc_distrito, desc_barrio;
```

### 5. Trabajando con tablas en HDFS.

5.1) Crear un documento de texto en el almacenamiento local que contenga una secuencia de números distribuidos en filas y separados por columnas, llámalo datos1.

```console
[cloudera@quickstart Desktop]$ cat datos1.txt
1,2,3
4,5,6
7,8,9
```

5.2) Crear un segundo documento (datos2) con otros números pero la misma estructura.

```console
[cloudera@quickstart Desktop]$ cat datos2.txt
11,12,13
14,15,16
17,18,19
```

5.3) Crear un directorio en HDFS con un nombre a placer, por ejemplo, /test.

```console
[cloudera@quickstart ~]$ hdfs dfs -mkdir /user/cloudera/test
```

5.4) Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando desde consola.

```console
[cloudera@quickstart ~]$ hdfs dfs -put /Desktop/datos1.txt /user/cloudera/test/
```

5.5) Desde Hive, crea una nueva database por ejemplo con el nombre numeros. Crea una tabla que no sea externa y sin argumento location con tres columnas numéricas, campos separados por coma y delimitada por filas. La llamaremos por ejemplo numeros_tbl.

```sql
CREATE DATABASE IF NOT EXISTS numeros;

CREATE TABLE numeros_tbl(
    num1 int,
    num2 int,
    num3 int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
```

5.6) Carga los datos de nuestro fichero de texto datos1 almacenado en HDFS en la tabla de Hive. Consulta la localización donde estaban anteriormente los datos almacenados. ¿Siguen estando ahí? ¿Dónde están?. Borra la tabla, ¿qué ocurre con los datos almacenados en HDFS?

```sql
LOAD DATA INPATH '/user/cloudera/test/datos1.txt' INTO TABLE numeros_tbl;
```

Los datos se encuentran en el Warehouse de Hive.

```sql
DROP TABLE IF EXISTS numeros_tbl;
```

Al borrar la tabla los datos se pierden.

5.7) Vuelve a mover el fichero de texto datos1 desde el almacenamiento local al
directorio anterior en HDFS.

```console
[cloudera@quickstart ~]$ hdfs dfs -put /Desktop/datos1.txt /user/cloudera/test/
```

5.8) Desde Hive, crea una tabla externa sin el argumento location. Y carga datos1 (desde HDFS) en ella. ¿A dónde han ido los datos en HDFS? Borra la tabla ¿Qué ocurre con los datos en hdfs?

```sql
CREATE EXTERNAL TABLE numeros_tbl(
    num1 int,
    num2 int,
    num3 int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/user/cloudera/test/datos1.txt' INTO TABLE numeros_tbl;
```

La tabla sigue estando en el Warehouse de Hive, pero al borrarla no se pierden los datos.

5.9) Borra el fichero datos1 del directorio en el que estén. Vuelve a insertarlos en el directorio que creamos inicialmente (/test). Vuelve a crear la tabla numeros desde hive pero ahora de manera externa y con un argumento location que haga referencia al directorio donde los hayas situado en HDFS (/test). No cargues los datos de ninguna manera explícita. Haz una consulta sobre la tabla que acabamos de crear que muestre todos los registros. ¿Tiene algún contenido?

```console
[cloudera@quickstart ~]$ hdfs dfs -rm /user/hive/warehouse/numeros.db/numeros_tbl/datos1
```

```console
[cloudera@quickstart ~]$ hdfs dfs -put Desktop/datos1.txt /user/cloudera/test/
```

```sql
CREATE EXTERNAL TABLE numeros_tbl(
    num1 int,
    num2 int,
    num3 int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/test';

SELECT * FROM numeros_tbl;
```

Muestra el contenido del fichero datos1.txt.

5.10) Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra?

```console
[cloudera@quickstart ~]$ hdfs dfs -put /Desktop/datos2.txt /user/cloudera/test/
```

```sql
SELECT * FROM numeros_tbl;
```

Muestra el contenido de los ficheros datos1.txt y datos2.txt.

5.11) Extrae conclusiones de todos estos anteriores apartados.

Al crear una tabla interna los datos se borran al eliminar dicha tabla, por el contrario, cuando creamos una externa, solo se borran los metadatos. Si definimos un location, Hive usará todos los ficheros localizados en dicho location.