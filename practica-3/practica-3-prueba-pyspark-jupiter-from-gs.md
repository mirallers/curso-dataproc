# Práctica 3: ejecutar un job pyspark en un cuaderno de Jupyter notebooks

En esta práctica vamos a calcular un aumento del 10% de los sueldos de los trabajadores de una compañía
## Pasos a seguir:
### Opción 1:
### 1.- Acceder al notebook Jupyter

Vamos a **Dataproc -> Clústeres -> Seleccionamos nuestro cluster -> interfaces web -> JupyterLab -> python 3 Notebook** 

### 2.- Creamos un bucket de Cloud storage llamado 'csv' en el bucket asociado a nuestro cluster de dataproc.


### 3.- Subimos salarios.csv a 'csv y obtenemos la ruta absoluta:


### 4.- volvemos al Notebook y creamos un script de pyspark que tenga esta forma:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
# Crear una sesión de Spark
spark = #aqui va una creacion de una sesion de spark

# Variables
input_path = #ruta de salarios.csv
output_path = #ruta de aumentos.csv

# Leer el archivo CSV
df = #se lee salarios.csv con sus encabezados

# Agregar una nueva columna con un aumento del 10% en el salario
df_transformed = # dicha columna nueva se llamara 'salario_aumentado'

# Mostrar los datos transformados
# Aqui se muestra el dataframe transformado


# Aqui escribimos el dataframe transformado al que llamaremos aumentos.csv
# en la ruta de aumentos.csv

```

### 5.- Comprobamos que haya escrito los datos
- Vamos a **Cloud storage -> Buckets -> seleccionamos nuestro bucket -> csv -> aumentos.csv**
