# Práctica 3: ejecutar un job pyspark en un cuaderno de Jupyter notebooks

En esta práctica vamos a calcular un aumento del 10% de los sueldos de los trabajadores de una compañía
## Pasos a seguir:
### Opción 1:
### 1.- Acceder al notebook Jupyter

Vamos a **Dataproc -> Clústeres -> Seleccionamos nuestro cluster -> interfaces web -> JupyterLab -> python 3 Notebook** 

### 2.- Creamos un bucket de Cloud storage llamado csv.

- Sacamos el nombre del bucket asociado a nuestro cluster **Dataproc -> Clústeres -> configuración** y buscamos 

|atributo|valor|
|---|---|
|Bucket de etapa de pruebas de Cloud Storage|```dataproc-<region>-<cluster-name>-<random-suffix>```|

- Vamos a **Cloud storage -> Buckets -> seleccionamos nuestro bucket -> Crear carpeta -> csv**

### 3.- Subimos nuestro csv y obtenemos ruta absoluta:

- Clicamos en **Subir -> salarios.csv**
- Clicamos en **salarios.csv** y buscamos 

|atributo|valor|
|---|---|
|URI de gsutil|```gs://dataproc-<region>-<cluster-name>-<random-suffix>/csv/salarios.csv```|


### 4.- volvemos al Notebook y pegamos nuestro código y ejecutamos:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder.appName("ProcesarSalarios").getOrCreate()

# Variables
input_path = "gs://dataproc-<region>-<cluster-name>-<random-suffix>/csv/salarios.csv"
output_path = "gs://dataproc-<region>-<cluster-name>-<random-suffix>/csv/aumentos.csv"

# Leer el archivo CSV
df = spark.read.option("delimiter", ";").option("encoding", "ISO-8859-1").csv(input_path, header=True, inferSchema=True)

# Agregar una nueva columna con un aumento del 10% en el salario
df_transformed = df.withColumn("salario_aumentado", col("salario") + col("salario") * 0.1)

# Mostrar los datos transformados
df_transformed.show()

df_transformed.write.option("delimiter", ";").option("enconding", "ISO-8859-1").csv(output_path, header=True, mode="overwrite")
```

### 5.- Comprobamos que haya escrito los datos
- Vamos a **Cloud storage -> Buckets -> seleccionamos nuestro bucket -> csv -> aumentos.csv**
