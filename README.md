# Coffee Shop Analysis - Sistema de Procesamiento Distribuido

Este proyecto implementa un sistema de procesamiento distribuido para analizar datos de transacciones de una cafetería usando RabbitMQ como middleware de mensajería.

## Query Implementada

**Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las 11:00 PM con monto total mayor o igual a 75.**

## Arquitectura del Sistema

### Componentes

1. **Cliente**: Envía datos CSV al gateway mediante sockets TCP
2. **Gateway**: Recibe datos del cliente y los envía a RabbitMQ
3. **Workers Especializados**:
   - **Year Filter Worker**: Filtra transacciones por año (2024, 2025)
   - **Time Filter Worker**: Filtra transacciones por hora (06:00 AM - 11:00 PM)
   - **Amount Filter Worker**: Filtra transacciones por monto (>= $75)
   - **Results Worker**: Muestra los resultados finales

### Flujo de Datos

```
Cliente → Gateway → RabbitMQ → Year Filter → Time Filter → Amount Filter → Results
```

## Estructura de Archivos

```
src/
├── client/                 # Cliente que envía datos CSV
├── gateway/               # Gateway que recibe datos y los envía a RabbitMQ
├── workers/               # Workers especializados
│   ├── year_filter_worker.py
│   ├── time_filter_worker.py
│   ├── amount_filter_worker.py
│   ├── results_worker.py
│   └── start_workers.py
└── middleware/            # Middleware RabbitMQ
    └── rabbitmq_middleware.py
```

## Instalación y Uso

### Prerrequisitos

- Docker y Docker Compose
- Python 3.11+

### Ejecutar el Sistema

1. **Iniciar todos los servicios**:
```bash
docker-compose up --build
```

2. **Verificar que todos los servicios estén funcionando**:
```bash
docker-compose ps
```

3. **Ver logs de los workers**:
```bash
# Ver logs de todos los workers
docker-compose logs -f

# Ver logs de un worker específico
docker-compose logs -f year-filter-worker
docker-compose logs -f time-filter-worker
docker-compose logs -f amount-filter-worker
docker-compose logs -f results-worker
```

### Probar el Sistema

1. **Colocar archivos CSV en la carpeta de datos**:
```bash
# Crear estructura de carpetas
mkdir -p src/client/.data/transactions
mkdir -p src/client/.data/users
mkdir -p src/client/.data/transaction_items

# Colocar archivos CSV en las carpetas correspondientes
# - transactions_202307.csv en src/client/.data/transactions/
# - users_202307.csv en src/client/.data/users/
# - transaction_items_202307.csv en src/client/.data/transaction_items/
```

2. **El cliente se ejecutará automáticamente** y enviará los datos al gateway.

3. **Los workers procesarán las transacciones** y mostrarán los resultados.

## Configuración

### Variables de Entorno

- `RABBITMQ_HOST`: Host de RabbitMQ (default: localhost)
- `RABBITMQ_PORT`: Puerto de RabbitMQ (default: 5672)
- `GATEWAY_HOST`: Host del gateway (default: localhost)
- `GATEWAY_PORT`: Puerto del gateway (default: 12345)

### Colas de RabbitMQ

- `transactions_raw`: Cola de entrada para transacciones
- `transactions_year_filtered`: Cola intermedia después del filtro de año
- `transactions_time_filtered`: Cola intermedia después del filtro de hora
- `transactions_final_results`: Cola final con resultados

## Desarrollo

### Ejecutar Workers Individualmente

```bash
# Navegar a la carpeta de workers
cd src/workers

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar un worker específico
python year_filter_worker.py
python time_filter_worker.py
python amount_filter_worker.py
python results_worker.py

# O ejecutar todos los workers
python start_workers.py
```

### Estructura de Datos

#### Transacciones
```json
{
  "transaction_id": "string",
  "store_id": "int",
  "payment_method_id": "int", 
  "voucher_id": "string",
  "user_id": "int",
  "original_amount": "float",
  "discount_applied": "float",
  "final_amount": "float",
  "created_at": "YYYY-MM-DD HH:MM:SS"
}
```

#### Resultados Finales
```json
{
  "transaction_id": "string",
  "final_amount": "float",
  "original_amount": "float", 
  "discount_applied": "float",
  "created_at": "YYYY-MM-DD HH:MM:SS"
}
```

## Monitoreo

### RabbitMQ Management UI

Acceder a http://localhost:15672 para monitorear las colas y mensajes.

- Usuario: guest
- Contraseña: guest

### Logs del Sistema

```bash
# Ver todos los logs
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f gateway
docker-compose logs -f client
docker-compose logs -f year-filter-worker
```

## Troubleshooting

### Problemas Comunes

1. **Workers no se conectan a RabbitMQ**:
   - Verificar que RabbitMQ esté funcionando: `docker-compose logs rabbitmq`
   - Verificar variables de entorno

2. **No se procesan transacciones**:
   - Verificar que el cliente esté enviando datos
   - Verificar logs del gateway

3. **No se muestran resultados**:
   - Verificar que los workers estén funcionando
   - Verificar logs de cada worker

### Limpiar el Sistema

```bash
# Detener todos los servicios
docker-compose down

# Limpiar volúmenes
docker-compose down -v

# Reconstruir imágenes
docker-compose up --build --force-recreate
```