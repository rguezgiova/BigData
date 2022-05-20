Ejercicio 1. ¿Qué pasaría si cancelamos (utilizando Ctrl+C) uno de los consumidores (quedando 2) y seguimos enviando mensajes por el producer?

Los mensajes se siguen repartiendo entre los dos restantes.

Ejercicio 2. ¿Qué pasaría si cancelamos otro de los consumidores (quedando ya solo 1) y seguimos enviando mensajes por el producer?

Los mensajes ya no se reparten y llegan en su totalidad al único nodo restante.

Ejercicio 3. ¿Qué sucede si lanzamos otro consumidor, pero está vez de un grupo llamado my-second-application leyendo el topic desde el principio (--from-beginning)?

Que se visualizan todos los mensajes enviados anteriormente.

Ejercicio 4. Cancela el consumidor y ¿Qué sucede si lanzamos de nuevo el consumidor, pero formando parte del grupo my-second-application?¿Aparecen los mensajes desde el principio?

Esta segunda vez ya no aparecerían los mensajes anteriores.

Ejercicio 5. Cancela el consumer, a su vez aprovecha de enviar más mensajes utilizando el producer y de nuevo lanza el consumidor formando parte del grupo my-second_application ¿Cuál fue el resultado?

Aparecerían los mensajes enviados entre la parada y el nuevo lanzamiento.

### Ejercicio Final

Crear topic llamado "topic_app" con 3 particiones y replication-factor = 1.

```console
spark@spark-virtualBox:~$ kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic topic_app --create --partitions 3 --replication-factor 1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "topic_app".
```

Crear un producir que inserte mensajes en el topic recién creado (topic_app).

```console
spark@spark-virtualBox:~$ kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic topic_app
>
```

Crear 2 consumer que forman parte de un grupo de consumo llamado "my_app".

```console
spark@spark-virtualBox:~$ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic topic_app --group my_app
```

Interactuar con los 3 elementos creando mensajes en el producir y visualizar como estos son consumidos por los consumer.

```console
spark@spark-virtualBox:~$ kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic topic_app
>Mensaje 1
>Mensaje 2
>Mensaje 3
>Mensaje 4
>Mensaje 5
```

```console
spark@spark-virtualBox:~$ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic topic_app --group my_app
Mensaje 1
Mensaje 2
Mensaje 4
Mensaje 5
```

```console
spark@spark-virtualBox:~$ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic topic_app --group my_app
Mensaje 3
```

Aplicar los comandos necesarios para listar los topics, grupos de consumo, así como describir cada uno de estos.

```console
spark@spark-virtualBox:~$ kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
first_topic
new_topic
topic_app
```

```console
spark@spark-virtualBox:~$ kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic topic_app --describe
Topic:topic_app	PartitionCount:3	ReplicationFactor:1	Configs:
	Topic: topic_app	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
	Topic: topic_app	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
	Topic: topic_app	Partition: 2	Leader: 0	Replicas: 0	Isr: 0
```

```console
spark@spark-virtualBox:~$ kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group my_app

TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                     HOST            CLIENT-ID
topic_app       0          4               4               0               consumer-1-3e045740-d955-4e81-9a00-47ee2e18de11 /127.0.0.1      consumer-1
topic_app       1          3               3               0               consumer-1-3e045740-d955-4e81-9a00-47ee2e18de11 /127.0.0.1      consumer-1
topic_app       2          3               3               0               consumer-1-80c82a0c-0af0-4612-8663-d028e73014b2 /127.0.0.1      consumer-1
```