#!/bin/bash

echo "Iniciando prueba de comunicación entre contenedores Docker..."
echo "Comunicación 1 a 1: Producer -> Queue -> Consumer"
echo ""

# Verificar que Docker esté corriendo
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker no está corriendo. Por favor, inicia Docker primero."
    exit 1
fi

# Limpiar contenedores anteriores
echo "Limpiando contenedores anteriores..."
docker-compose -f docker-compose-test.yml down --remove-orphans

# Construir y ejecutar
echo "Construyendo y ejecutando contenedores..."
echo ""

# Ejecutar en orden: RabbitMQ -> Consumer -> Producer
echo "1. Iniciando RabbitMQ..."
docker-compose -f docker-compose-test.yml up -d rabbitmq

echo "Esperando que RabbitMQ esté listo..."
sleep 10

echo "2. Iniciando Consumer..."
docker-compose -f docker-compose-test.yml up -d consumer

echo "Esperando que Consumer esté listo..."
sleep 5

echo "3. Iniciando Producer..."
docker-compose -f docker-compose-test.yml up producer

echo ""
echo "Verificando logs..."

echo ""
echo "Logs del Consumer:"
docker-compose -f docker-compose-test.yml logs consumer

echo ""
echo "Logs del Producer:"
docker-compose -f docker-compose-test.yml logs producer

echo ""
echo "Limpiando contenedores..."
docker-compose -f docker-compose-test.yml down

echo "Prueba completada!"
