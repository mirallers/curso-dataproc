# Práctica 1: Crear un publicador universal de pub/sub de GCP.

En esta práctica vamos a crear un publicador de datos generico para el servicio de pub/sub de GCP.

---

## **Estructura del Proyecto**

La estructura del proyecto es la siguiente:

```BASH
publisher/
 |____config/
 |       |____ service_account.json    # Clave de la cuenta de servicio de 
 |
 |____scripts/
         |____ publisher.py            # Script Python para el publicador

```

## Pasos a seguir

### 1.- Crear una cuenta de servicio

- Ve a la consola de GCP: [Google Cloud Console](https://console.cloud.google.com).
- Dirígete a "IAM y administración" > "Cuentas de servicio".
- Haz clic en "Crear cuenta de servicio" y sigue los pasos.
- Asigna a la cuenta el rol de **Publicador de Pub/Sub**.
- Una vez creada la cuenta, haz clic en "Administrar claves" > "Agregar clave" > "Crear clave".
- Descarga la clave en formato JSON.
- Guarda el archivo JSON en la carpeta `config/` del proyecto y renómbralo como `service_account.json`.

---

### 2.- Instala el cliente de Pub/Sub:
```bash
pip install google-cloud-pubsub
```

### 3.- Crear el script publisher.py
Dentro de la carpeta `scripts/`, crea un archivo llamado `publisher.py`.
Agrega el siguiente código:

```python
import os
from google.cloud import pubsub_v1

def publish_message(project_id, topic_id, credentials_path, message):
    """
    Publica un mensaje en un tópico de Google Pub/Sub.

    Args:
        project_id (str): ID del proyecto de Google Cloud.
        topic_id (str): ID del tópico en Pub/Sub.
        credentials_path (str): Ruta al archivo JSON de credenciales de la cuenta de servicio.
        message (str): Mensaje a publicar.

    Returns:
        str: ID del mensaje publicado.
    """
    # Crear el cliente de Publisher desde el archivo de credenciales
    publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_path)

    # Construir la ruta completa del tópico
    topic_path = f"projects/{project_id}/topics/{topic_id}"

    # Codificar el mensaje y publicarlo
    data = message.encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()


if __name__ == "__main__":
    # Datos de prueba
    project_id = "pub-sub-subcriber-bigquery"  # Reemplaza con tu ID de proyecto
    topic_id = "mi-topico"  # Reemplaza con tu ID de tema
    message = "Hola, este es un mensaje de prueba."

    # Obtener la ruta absoluta al archivo de credenciales
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directorio actual del script
    credentials_path = os.path.abspath(os.path.normpath(os.path.join(script_dir, '..', 'config', 'service_account.json')))

    try:
        # Llamar a la función para publicar el mensaje
        message_id = publish_message(project_id, topic_id, credentials_path, message)
        print(f"Mensaje publicado con éxito. ID del mensaje: {message_id}")
    except Exception as e:
        print(f"Error al publicar el mensaje: {e}")
```

### 4.- Configurar el tema en GCP
Ve a la consola de GCP y crea un tema en Pub/Sub:
Navega a "Pub/Sub" > "Temas".
Haz clic en "Crear tema" y anota el ID del tema para usarlo en el script.

### 5.- Apuntar el id del proyecto.

### 6.- Lanzar el script y verlo en el tópico que hemos creado.


















