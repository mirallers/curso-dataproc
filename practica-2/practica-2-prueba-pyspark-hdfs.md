# Práctica 2: ejecutar un job pyspark alojado en una ruta de hdfs

## Pasos a seguir:

### 1.- Acceder a la máquina mediante SSH.

Vamos a **Compute Engine -> instacias de VM -> prueba-cluster-m -> SSH**

### 2.- Creamos nuestro archivo o lo subimos directamente.

Aqui lo crearemos con un

```bash
vim prueba-pyspark-hdfs.py
```

### 3.- Pegamos el código:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicia una sesion de Spark
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://<namenode-hostname>:8020") \
    .getOrCreate()

# Datos de ejemplo (puedes reemplazarlos con datos de HDFS si es necesario)
data = [("Alice", 1), ("Bob", 2), ("Alice", 3), ("Bob", 4), ("Alice", 5)]
columns = ["name", "value"]

# Crea un DataFrame
df = spark.createDataFrame(data, columns)

# Realiza una operacion de agregacion (contar la suma de "value" por "name")
df_grouped = df.groupBy("name").sum("value")

# Muestra los resultados
df_grouped.show()

# detenemos la sesion de Spark
spark.stop()

```
y escribimos para guardar

```bash
:wq
```

### 4.- Cambiamos los permisos

```bash
chmod 777 prueba-pyspark-hdfs.py
```

### 5.- Creamos la ruta de ejecución de HDFS

```bash
hdfs dfs -mkdir /scripts
```

### 6.- Damos permisos 777 a dicha carpeta

```bash
hdfs dfs -chmod 777 /scripts
```

### 7.- Subimos el archivo 

```bash
hdfs dfs -put  prueba-pyspark-hdfs.py /scripts/
```

### 8.- Subimos el archivo 

```bash
hdfs dfs -chmod 777 /scripts/prueba-pyspark-hdfs.py
```


### En la parte de dataproc:

Vamos a **Dataproc -> trabajos -> Enviar trabajo** 

|opciones|poner|
|:---|----|
|Tipo de trabajo|``Pyspark``|`
|archivo principal|``hdfs:///scripts/prueba-pyspark-hdfs.py`` |

