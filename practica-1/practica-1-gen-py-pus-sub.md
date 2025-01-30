# Práctica 1: Crear un publicador universal de pub/sub de GCP.

En esta práctica vamos a crear un publicador de datos generico para el servicio de pub/sub de GCP.

---

## **Estructura del Proyecto**

La estructura del proyecto es la siguiente:

```BASH
publisher/
 |____config/
 |       |____ service_account.json    # Clave de la cuenta de servicio de escritura en el topico de Pub/Sub

 |
 |____scripts/
         |____ publisher.py            # Script Python para el publicador

```

## Pasos a seguir

### 1.- Debemos crear una cuenta de servicio que tenga permisos de escritura y obtener la clave en formato json.

### 2.- Dentro de publisher.py

- Instalar módulo de pub/sub en python.
- Crear una función llamada **publish_message** con parámetros:
    - project_id
    - topic_id
    - credentials_path
    - message

### 3.- Ejecutarlo y ver si ha llegado el mensaje.


















