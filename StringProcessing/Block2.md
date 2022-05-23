Ejercicio 1. Crea un nuevo topic que maneje datos con formato JSON con variedad de atributos.

```console
spark@spark-virtualBox:~$ kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic json_variety --create --partitions 3 --replication-factor 1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "json_variety".
```

```json
{
    "tableName": "json_variety",
    "topicName": "json_variety",
    "schemaName": "default",
    "message": {
        "dataFormat":"json",
        "fields": [
            {
                "name": "nombre",
                "mapping": "nombre",
                "type": "VARCHAR"
            },
            {
                "name": "fechaEntrada",
                "mapping": "fechaEntrada",
                "type": "DATE"
            },
            {
                "name": "fechaSalida",
                "mapping": "fechaSalida",
                "type": "DATE"
            },
            {
                "name": "habitacion",
                "mapping": "habitacion",
                "type": "INT"
            },
            {
                "name": "reserva",
                "mapping": "reserva",
                "type": "BOOLEAN"
            },
            {
                "name": "precio",
                "mapping": "precio",
                "type": "DOUBLE"
            }
        ]
    }
}
```

Ejercicio 2. Inserta varios registros (al menos unos 6-10 registros) y crea una tabla en presto asociada a ese topic.

```json
{"nombre":"Giovanni", "fechaEntrada":"2022-05-23", "fechaSalida":"2022-06-01", "habitacion":201, "reserva":true, "precio":220.60}    
{"nombre":"Laura", "fechaEntrada":"2022-04-23", "fechaSalida":"2022-05-23", "habitacion":308, "reserva":true, "precio":800}
{"nombre":"Carlos", "fechaEntrada":"2022-06-01", "fechaSalida":"2022-06-02", "habitacion":202, "reserva":false, "precio":50.30}
{"nombre":"Pedro", "fechaEntrada":"2022-07-27". "fechaSalida":"2022-07-30", "habitacion":408, "reserva":true, "precio":120.27}
{"nombre":"Jose", "fechaEntrada":"2022-08-01", "fechaSalida":"2022-08-07", "habitacion":201, "reserva":true, "precio":120}
{"nombre":"Maria", "fechaEntrada":"2022-05-23", "fechaSalida":"2022-05-30", "habitacion":402, "reserva":false, "precio":200.76}
```

Ejercicio 3. Crea una tabla en presto asociada a ese topic.

```console
connector.name=kafka
kafka.nodes=localhost:9092
kafka.table-names=text_topic,csv_topic,json_topic,json_variety
kafka.hide-internal-columns=false
```

Ejercicio 4. Lanza consultas sobre el topic (que incluso podría estar en una continua evolución, asumiendo que sigan insertando elementos) con condiciones (cláusulas WHERE) basadas en algunos de los atributos que contienen dichos JSON.

```sql
presto:default> SHOW TABLES;
    Table     
--------------
 csv_topic    
 json_topic   
 json_variety 
 text_topic   
(4 rows)

Query 20220523_100138_00002_emkdk, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
0:00 [4 rows, 109B] [16 rows/s, 438B/s]
```

```sql
presto:default> SELECT nombre, fechaentrada, fechasalida, habitacion, reserva, precio FROM json_variety;
  nombre  | fechaentrada | fechasalida | habitacion | reserva | precio 
----------+--------------+-------------+------------+---------+--------
 Laura    | 2022-04-23   | 2022-05-23  |        308 | true    |  800.0 
 Jose     | 2022-08-01   | 2022-08-07  |        201 | true    |  120.0 
 Carlos   | 2022-06-01   | 2022-06-02  |        202 | false   |   50.3 
 Maria    | 2022-05-23   | 2022-05-30  |        402 | false   | 200.76 
 Giovanni | 2022-05-23   | 2022-06-01  |        201 | true    |  220.6 
 Pedro    | 2022-07-27   | 2022-07-30  |        408 | true    | 120.27 
(6 rows)

Query 20220523_115432_00010_q5hzz, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
0:00 [6 rows, 880B] [53 rows/s, 6,58KB/s]
```

```sql
presto:default> SELECT nombre, fechaentrada, fechasalida, habitacion, reserva, precio FROM json_variety WHERE reserva = true;
  nombre  | fechaentrada | fechasalida | habitacion | reserva | precio 
----------+--------------+-------------+------------+---------+--------
 Laura    | 2022-04-23   | 2022-05-23  |        308 | true    |  800.0 
 Jose     | 2022-08-01   | 2022-08-07  |        201 | true    |  120.0 
 Giovanni | 2022-05-23   | 2022-06-01  |        201 | true    |  220.6 
 Pedro    | 2022-07-27   | 2022-07-30  |        408 | true    | 120.27 
(4 rows)

Query 20220523_115943_00011_q5hzz, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
0:00 [7 rows, 880B] [37 rows/s, 4,58KB/s]
```

```sql
presto:default> SELECT nombre, fechaentrada, fechasalida, habitacion, reserva, precio FROM json_variety WHERE precio < 200 ;
 nombre | fechaentrada | fechasalida | habitacion | reserva | precio 
--------+--------------+-------------+------------+---------+--------
 Jose   | 2022-08-01   | 2022-08-07  |        201 | true    |  120.0 
 Carlos | 2022-06-01   | 2022-06-02  |        202 | false   |   50.3 
 Pedro  | 2022-07-27   | 2022-07-30  |        408 | true    | 120.27 
(3 rows)

Query 20220523_120337_00012_q5hzz, FINISHED, 1 node
Splits: 19 total, 19 done (100,00%)
0:00 [7 rows, 880B] [34 rows/s, 4,21KB/s]
```