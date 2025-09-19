# Middleware de RabbitMQ para Sistemas Distribuidos

Este proyecto implementa un middleware de comunicación basado en RabbitMQ siguiendo la interfaz proporcionada por la cátedra. El middleware soporta diferentes patrones de comunicación para sistemas distribuidos.

## Instalación

### Prerrequisitos

- Docker y Docker Compose
- Python 3.11+

### Configuración

1. Clonar el repositorio
2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

## Uso

### Ejecutar RabbitMQ con Docker

```bash
# Iniciar RabbitMQ
docker-compose up rabbitmq -d

# Verificar que esté funcionando
docker-compose ps
```

### Demostración Manual

```bash
# Ejecutar script de demostración
python demo_middleware.py
```

### Ejecutar Pruebas

```bash
# Ejecutar todas las pruebas
./run_tests.sh

# O ejecutar manualmente
docker-compose run --rm test-env
```

## Patrones de Comunicación Soportados

### 1. Working Queue 1 a 1

```python
from middleware import RabbitMQMiddlewareQueue

# Producer
producer = RabbitMQMiddlewareQueue("localhost", "mi_cola")
producer.send({"mensaje": "Hola mundo"})

# Consumer
consumer = RabbitMQMiddlewareQueue("localhost", "mi_cola")
def callback(mensaje):
    print(f"Recibido: {mensaje}")

consumer.start_consuming(callback)
```

### 2. Working Queue 1 a N (Competing Consumers)

```python
# Múltiples consumers compitiendo por mensajes de la misma cola
consumer1 = RabbitMQMiddlewareQueue("localhost", "mi_cola")
consumer2 = RabbitMQMiddlewareQueue("localhost", "mi_cola")
consumer3 = RabbitMQMiddlewareQueue("localhost", "mi_cola")

# Cada consumer procesará mensajes de forma distribuida
```

### 3. Exchange 1 a 1

```python
from middleware import RabbitMQMiddlewareExchange

# Producer
producer = RabbitMQMiddlewareExchange("localhost", "mi_exchange", ["routing_key"])
producer.send({"mensaje": "Hola mundo"}, "routing_key")

# Consumer
consumer = RabbitMQMiddlewareExchange("localhost", "mi_exchange", ["routing_key"])
def callback(mensaje):
    print(f"Recibido: {mensaje}")

consumer.start_consuming(callback)
```

### 4. Exchange 1 a N

```python
# Múltiples consumers con diferentes routing keys
consumer1 = RabbitMQMiddlewareExchange("localhost", "mi_exchange", ["route1"])
consumer2 = RabbitMQMiddlewareExchange("localhost", "mi_exchange", ["route2"])
consumer3 = RabbitMQMiddlewareExchange("localhost", "mi_exchange", ["route1", "route2"])

# Enviar mensajes a diferentes routing keys
producer.send({"mensaje": "Para route1"}, "route1")
producer.send({"mensaje": "Para route2"}, "route2")
```

## Monitoreo

Accede a la interfaz de gestión de RabbitMQ en:
- URL: http://localhost:15672
- Usuario: guest
- Contraseña: guest

## Referencias

- [Tutoriales oficiales de RabbitMQ](https://www.rabbitmq.com/tutorials)
- [Documentación de Pika](https://pika.readthedocs.io/)
- [Patrones de integración empresarial](http://www.enterpriseintegrationpatterns.com/)
