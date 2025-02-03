# Práctica 4: ejecutar un job pyspark en un cuaderno de Jupyter notebooks

En esta práctica vamos a procesar crear proceso end to end, es decir, vamos a recoger datos de una fuente de datos, procesarla
e ingestarla dentro de una tabla de bigquery.


## Pasos a seguir:

### 1.- Crear proyecto llamado `pub-sub-subscriber-bigquery`
Vamos a **Seleciona un proyecto -> Proyecto nuevo ->**
-   Nombre del Proyecto: `pub-sub-subscriber-bigquery`
-   Clicamos en `Crear`

### 1.1.- Habilitar las APIs de Pub/Sub, Dataproc y BigQuery.
Además deberemos dar los permisos a las cuentas de servicio asociadas al cluster de dataproc desde IAM
- **pub/sub**: 
    -   Publicador de Pub/Sub
    -   Suscriptor de Pub/Sub
- **bigquery**:
    -   Administrador de BigQuery.

### 2.- Crear un tópico llamado `topico-datos`
Hay dos formas de hacerlo:
- #### **Usando la interfaz gráfica**:
    Vamos a **Pub/Sub -> Temas -> Crear Tema ->**

    -    ID del tema: `topico-datos`
    -    Clicamos en `agregar una suscipción prederteminada`.
    -    Clicamos en `Clave de encriptación administrada por Google`.
    -    Clicamos en `Crear`

- #### **Usando Cloud Shell de GCP**:
    Vamos a **Pub/Sub -> Cloud Shell ->**
    -   Creamos el tópico.
        ```shell
        gcloud pubsub topics create topico-datos
        ```
    -   Creamos la subscripción.
        ```shell
        gcloud pubsub subscriptions create topico-datos-sub --topic=topico-datos
        ```   
### 3.- Crear un generador de datos en python que se ejecute en un notebook de jupyter
```python

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import random
import json
from datetime import datetime
from google.cloud import pubsub_v1


def generate_random_username():
    names = [
    "alphaWolf", "betaBear", "gammaFox", "deltaHawk", "epsilonStag", 
    "zetaLynx", "thetaCrow", "iotaFalcon", "kappaOtter", "lambdaPanther", 
    "muJaguar", "nuCoyote", "xiEagle", "omicronTiger", "piPuma", 
    "rhoBadger", "sigmaRaven", "tauOrca", "upsilonSeal", "phiHeron", 
    "chiCougar", "psiWolfhound", "omegaBison", "shadowLark", "stormPike", 
    "emberDove", "frostMink", "blazeViper", "thornJackal", "ironElk", 
    "swiftWolverine", "silentOwl", "nightVulture", "braveRook", "wildFalcon", 
    "goldenHawk", "crimsonLynx", "jadePanther", "onyxFox", "azureWolf", 
    "quartzRaven", "arcticSwan", "scarletBear", "electricStag", "lunarCougar", 
    "stellarOcelot", "solarFox", "duskRaven", "dawnTiger", "voidCoyote"
    ]
    return random.choice(names)

def generate_random_action():
    actions = ['PUBLICAR', 'ENVIAR', 'COMENTAR']
    action = random.choice(actions)
    if action == 'PUBLICAR':
        sub_action = random.choice(['FOTO', 'VIDEO'])
    elif action == 'ENVIAR':
        sub_action = random.choice(['MENSAJE', 'FOTO', 'VIDEO'])
    elif action == 'COMENTAR':
        sub_action = 'MENSAJE'
    return action, sub_action

def generate_random_metadata(sub_action):
    if sub_action == 'FOTO':
        resolution = f"{random.randint(720, 4320)}p"
        size = f"{random.uniform(0.5, 10):.2f}MB"
        return resolution + '-' + str(size)
    elif sub_action == 'VIDEO':
        resolution = f"{random.randint(720, 4320)}p"
        duration = random.randint(10, 300)  # duración en segundos
        size = f"{random.uniform(5.0, 500):.2f}MB"
        return resolution + '-' + str(duration) + '-' + str(size)
    elif sub_action == 'MENSAJE':
        length = random.randint(1, 500)  # longitud del mensaje en caracteres
        return str(length)

def generate_random_device():
    devices = ['IOS', 'WINDOWS', 'ANDROID']
    return random.choice(devices)

def generate_random_location():
    countries = ["USA", "Spain", "Mexico", "Canada", "Brazil", "France", "India", "China", "Australia", "Germany"]
    return random.choice(countries)

def generate_random_message():
    action, sub_action = generate_random_action()
    message = {
        "date_oprtn": datetime.now().isoformat(),
        "user": generate_random_username(),
        "action": action,
        "sub_action": sub_action,
        "metadata": generate_random_metadata(sub_action),
        "device": generate_random_device(),
        "location": generate_random_location()
    }
    return message

def calculate_total_messages(time_execution, type_time, wait_time_to_publish):
    # Convertir el tiempo de ejecución a segundos
    time_in_seconds = {'second': 1, 'minute': 60, 'hour': 3600}
    total_time_seconds = time_execution * time_in_seconds[type_time]
    return total_time_seconds // wait_time_to_publish

def calculate_and_generate_msg(time_execution, type_time, wait_time_to_publish):
    """

    Ejecuta la lógica de publicacion en un bucle con control de tiempo.

    Args:
        time_execution (int): Tiempo total para enviar mensajes.
        type_time (str): Unidad de tiempo ('second', 'minute', 'hour').
        wait_time_to_publish (int): Tiempo entre mensajes publicados.
    """

    # Creamos los datos
    total_messages = calculate_total_messages(time_execution, type_time, wait_time_to_publish)
    print(f"mensajes a enviar:{total_messages}")
    messages = [json.dumps(generate_random_message()) for _ in range(total_messages)]

    return messages

def publish_message(publisher, topic_path, message):
    """
    Publica un mensaje en Pub/Sub.

    Args:
        publisher (google.cloud.pubsub_v1.PublisherClient): Cliente de Pub/Sub.
        topic_path (str): Ruta del tópico de Pub/Sub.
        message (int): Número del mensaje a publicar.

    Returns:
        str: Resultado de la publicacion del mensaje.
    """
    data = message.encode("utf-8")
    future = publisher.publish(topic_path, data)
    return f"publicado: {future.result()} - {message}"
 
    
# Definimos las variables
project_name = 'pub-sub-subscriber-bigquery'
topic_name = 'topico-datos'

tiempo_total_ejecucion = 40
tipo_de_tiempo = 'second'
cada_cuanto_se_lanza = 2

# Conversiones de tiempo
time_conversion = {'second': 1, 'minute': 60, 'hour': 3600}

if tipo_de_tiempo not in time_conversion:
    raise ValueError("El parámetro 'tipo_de_tiempo' debe ser uno de ['second', 'minute', 'hour']")

# Convertir el tiempo total a segundos
total_time_seconds = tiempo_total_ejecucion * time_conversion[tipo_de_tiempo]
    
# Llamar a la funcion principal con los argumentos y la configuración
list_of_msg_to_send = calculate_and_generate_msg(
    time_execution=tiempo_total_ejecucion, 
    type_time=tipo_de_tiempo, 
    wait_time_to_publish=cada_cuanto_se_lanza
)

# Creamos la conexion con el cliente
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_name, topic_name)

start_time = time.time()
elapsed_time = 0
message_counter = 0

print(f"Publicando mensajes durante {total_time_seconds} segundos...")
while elapsed_time < total_time_seconds:
    # Publica un mensaje usando la funcion definida
    result = publish_message(publisher, topic_path, list_of_msg_to_send[message_counter])
    # Incrementar contador y esperar
    message_counter += 1
    time.sleep(cada_cuanto_se_lanza)
    elapsed_time = time.time() - start_time

print(f"Finalizó la publicacion. Total de mensajes enviados: {message_counter}")
```

### 4.- Crear base de datos y tablas en Bigquery:
-  Nombre de la base de datos: ``Nextgram``
    ```bash
    bq --location=US mk --dataset nextgram

    ```
-  Nombre de la base de datos: ``interaccion``
    ```bash
    bq mk --table nextgram.interacciones \
      date_partition:STRING,date_oprtn:TIMESTAMP,user:STRING,action:STRING,sub_action:STRING,metadata:STRING,device:STRING,location:STRING,id:STRING,control:STRING
    ```

donde la estructura de la tabla debe ser:

| Campo          | Atributo   |
|:---------------|:-----------|
|`date_partition`| STRING     | 
| `date_oprtn`   | TIMESTAMP  |
| `user`         | STRING     |
| `action`       | STRING     |
| `sub_action`   | STRING     |
| `metadata`     | STRING     |
| `device`       | STRING     |
| `location`     | STRING     |
| `id`           | STRING     |
| `control`      | STRING     |

### 5.- Crear un Subcriptor y procesador
En esta parte se debe hacer un script en python que se subcriba al tópico ``topico-datos` y que procese los mensajes de forma que haga las siguientes operaciones:
- Cree el campo `date_partition` a partir de la extracción del campo `date_oprtn` en formato `yyyy-mm-dd`.
- Cree un campo `id` que será el código alfanumérico tras hashear todos los campos anteriores con un SHA-256.
- Cree un campo `control` que sea las iniciales de su nombre (primer nombre, primer y segundo apellido)
    - ejemplo: **José Luis Martos García -> JMG -> control: `JMG`**
- Ingeste dichos datos en nuestra tabla ``interaccion``.


```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from google.cloud import pubsub_v1
import hashlib
import base64
from google.cloud import bigquery

# Definimos las variables
project_name = 'pub-sub-subscriber-bigquery'
subcription_name = 'topico-datos-sub'

# Función para procesar mensajes
def procesar_mensaje(mensaje):
    print(f"Recibido: {mensaje.data}")

    # Decodificar mensaje
    datos = json.loads(mensaje.data.decode("utf-8"))
    
    # Crear id único
    datos["date_partition"] = datos["date_oprtn"].split("T")[0]
    datos["id"] = hashlib.sha256(mensaje.data).hexdigest()
    datos["control"] = "MMN"

    # Insertar en BigQuery
    tabla = f"{project_name}.nextgram.interacciones"
    client_bq.insert_rows_json(tabla, [datos])
    print(f"Insertado en BigQuery: {datos}")
    
    # Confirmar el mensaje
    mensaje.ack()



# Configuración del cliente
subscriber = pubsub_v1.SubscriberClient()
subscription_path = f"projects/{project_name}/subscriptions/{subcription_name}"

client_bq = bigquery.Client()

# Suscripción
future = subscriber.subscribe(subscription_path, callback=procesar_mensaje)

print("Esperando mensajes...")
try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
```

### 6.- Verificar que los datos se están ingestando con una querie.
```sql
SELECT *
FROM `nextgram.interaccion`
LIMIT 10;
```