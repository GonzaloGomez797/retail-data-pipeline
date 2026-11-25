# Clase 11 · Redpanda con Python

El topic almacena eventos. El productor envía 100 mensajes y el consumidor muestra cada mensaje junto con su partición y su offset, que es su posición dentro de la partición.

## Preparación en PowerShell

Desde la raíz del proyecto en Visual Studio Code, ejecuta estos comandos. Puedes usar directamente el Python del entorno virtual sin activar el entorno.

```powershell
cd .\redpanda-python
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Pídele al profesor el servidor Kafka, el mecanismo SASL, el usuario, la contraseña y el topic asignado. Completa `.env` con esos datos. Para el ejercicio, el usuario necesita permiso de escritura y lectura sobre el topic. Si también debes crearlo, necesita permiso de creación.

## Ejecutar el ejercicio

```powershell
.\venv\Scripts\python.exe admin.py
.\venv\Scripts\python.exe producer.py
.\venv\Scripts\python.exe consumer.py
```

Si el profesor ya creó el topic, empieza por `producer.py`. Los tres scripts usan `KAFKA_TOPIC` de `.env` para apuntar al mismo topic.

El consumidor de esta práctica empieza en el primer offset disponible y no guarda su avance. Si lo vuelves a ejecutar, puede volver a leer los mensajes. Termina después de 60 segundos sin recibir mensajes.
