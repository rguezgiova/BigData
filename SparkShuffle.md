# Spark Shuffle

## Introducción
Shuffle es crucial para el rendimiento de una aplicación Spark, para entender y ver cuando se produce tendremos que observar
la ejecución a un nivel superior. Exploraremos el núcleo de Spark y como trabajan sus componentes para ejecutar los shuffles.

## ¿Qué es?
Shuffle es un puente entre mapear y reducir. Se mapea la salida para reducir la entrada. Durante este periodo la serialización
y deserialización, la entrada y salida de nodo cruzado y la entrada y salida de escritura y lectura del disco están involucradas.
Es una etapa muy costosa en el proceso de ejecución.

## ¿Cómo se produce?
Un shuffle se produce entre dos etapas, cuando DAGScheduler genera el plan de ejecución físico a partir del lógico, canaliza
los RDDs en una etapa en el que están conectados entre sí por una estrecha dependencia. En consecuencia corta el plan lógico
entre todos los RDDS con amplias dependencias. La etapa final que produce el resultado se llama ResultStage. Las etapas 
necesarias para calcular el ResultStage se llaman ShuffleMapStages.

**DIBUJO**

Por cada definición de un ShuffleMapStage debe producirse una redistribución de los datos antes de que se ejecute la siguiente
etapa. Cada etapa tiene que terminar con un shuffle.

## ¿Qué ocurre durante una etapa?
El plan físico generado se compone de etapas, que a su vez se componen de tareas. La evaluación de una etapa requiere la
evolución de todas las particiones del RDD final. Por lo tanto habrá una tarea para cada una de las particiones, por lo
tanto, el número de tareas es igual al número de particiones del RDD final.

Una vez que empezamos a evaluar el plan físico, Spark escanea hacia atrás a través de las etapas que faltan y que son
necesarias para calcular el resultado final. Cada etapa necesita proporcionar datos a las siguientes, el resultado de
ShuffleMapStage es crear un archivo de salida. Este archivo es particionado por la función de partición y es usado por
las etapas posteriores para obtener los datos requeridos. Por lo tanto, el shuffle se produce como un proceso implícito
que pega la ejecución posterior de etapas dependientes de las tareas.

Cuando se ejecuta una acción, los ejecutores de nuestro clúster no hacen nada diferente a la ejecución de las tareas.
Esta ejecución sigue un procedimiento general para cada tarea. Pasos para la ejecución de una sola ShuffleMapTask:

1) **Fetch**: El TaskScheduler programa una tarea para cada ejecutor. La tarea contiene que partición va a evaluar. El 
ejecutor accederá a los archivos de salida de las etapas anteriores para obtener las salidas de la partición respectiva.
Todo el proceso de obtención de los datos se encapsula en la memoria de los bloques ejecutores.


2) **Compute**: El ejecutor calcula el resultado de la salida del mapa para la partición aplicando las funciones de cadena.
Esto sigue siendo válido para los planes generados por WholeStageCodeGen de Spark, ya que produce un RDD con las funciones
fusionadas.


3) **Write**: El objetivo final de una tarea es producir un archivo particionado en el disco y registrarlo en el BlockManager
para proporcionarlo a las etapas posteriores. El archivo tiene que estar particionado para permitir que las etapas posteriores
obtengan solo, los datos necesarios para sus tareas. Esto es necesario, ya que la tarea subsiguiente obtendrá los registros
y aplicará la función de forma iterativa. Por lo tanto, las mismas claves deben aparecer consecutivamente.

## ¿Cómo se ejecuta una tarea?

**DIBUJO**

El dibujo muestra la ejecución principal de una ShuffleMapTask y los componentes más importantes que intervienen. Observando
el flujo de ejecución se hace muy evidente la estructura de una tarea general descrita anteriormente: Dentro del método
runTask se evalúa la partición del RDD final utilizando su iterador sobre los registros. Este iterador usa el método _compute_
del RDD que especifica cómo calcular la partición. Para un ShuffledRDD, se accede al BlockShuffleReader para leer los
datos necesarios. El BlockShuffleReader a su vez encapsula la distribución de los registros de la partición y es responsable
de acceder a la memoria de los bloques locales y remotos para proporcionar los registros solicitados. El BlockShuffleReader
utiliza el ShuffleBlockFetcherIterator que recupera iterativamente los bloques de los BlockManagers respetando los límites
de memoria y recuento. El iterador resultante se pasa directamente al ShuffleWriter.

Actualmente existen tres implementaciones de un escritor de shuffle, cada una con sus ventajas e inconvenientes. La elección
de una u otra depende principalmente del tipo de operación que queramos ejecutar y de la configuración. LA selección de
la estrategia respectiva se hace dentro de la única implementación del ShuffleManager por el momento: el SortShuffleManager.

## Shuffles y rendimiento
Si observamos la ejecución de las tareas desde esta perspectiva y nos damos cuenta de que no ocurre nada más en los
ejecutores que los tres pasos fundamentales descritos anteriormente, ver que estos son los únicos tres lugares donde se
puede producir cuellos de botella en el rendimiento. Todos esos pasos ocurren simultáneamente en cada ejecutor y estresan
a todos nuestros recursos:

- **CPU**: Utilizada para la evaluación de funciones, serialización, comprensión, encriptación, operaciones de lectura/escritura.
- **Memoria**: Utilizada por los buffers para realizar _fetch_ y _write_, pila de ejecución y pila de caché.
- **Red**: Utilizada mientras se leen y proporcionan los datos.
- **Disco**: Escribiendo resultados intermedios, cargando archivos de salida de mapas.

## Puntos importantes a tener en cuenta
1) Las particiones tienen un número estático de particiones shuffle.
2) Las particiones no cambian con el tamaño de los datos.
3) Si hay una cantidad de datos excesiva, reduce el procesamiento.
4) Si los datos son pequeños, no utiliza todos los recursos presentes en el clúster.

Para que no surjan esos problemas, las particiones se deben hacer de forma dinámica.