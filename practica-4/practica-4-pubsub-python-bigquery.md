# Práctica 4: ejecutar un job pyspark en un cuaderno de Jupyter notebooks

En esta práctica vamos a procesar crear proceso end to end, es decir, vamos a recoger datos de una fuente de datos, procesarla
e ingestarla dentro de una tabla de bigquery.


## Pasos a seguir:
### 1.- Crear proyecto llamado `pub-sub-subscriber-bigquery`

### 2.- Crear un tópico llamado `topico-datos`

### 3.- Crear un generador de datos en python que se ejecute en un notebook de jupyter
En esta parte tenéis que crear un script que envie datos a un tópico de pub/sub llamado "***topico-datos***"
donde el generador de datos cumpla las siguientes caracteristicas:

- La logica de ejecución debe ser:

  - 1 - En base a unos parámetros 
    - `time_execution`: *tiempo_total_ejecucion* (int)
    - `type_time`: *tipo_de_tiempo* ['second', 'minute', 'hour'] (string),
    - `wait_time_to_publish`: *cada_cuanto_se_envia* (int).
  
    se calcula:
    -    Número de datos a generar 
  - 2 - En base a ese número se generará tantos datos con el formato:
    ```json
    {
    "date_oprtn": "2025-01-26T18:34:57.409846",
    "user": "user9",
    "action": "ENVIAR",
    "sub_action": "MENSAJE",
    "metadata": "148",
    "device": "ANDROID",
    "location": "USA"
    }
    ```
    para ello se usará estos generadores:

    ```python
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
    ```

### 4.- Crear base de datos y tablas en Bigquery:
-  Nombre de la base de datos: ``Nextgram``
-  Nombre de la base de datos: ``interaccion``

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


### 6.- Verificar que los datos se están ingestando con una querie.