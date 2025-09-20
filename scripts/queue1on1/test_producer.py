#!/usr/bin/env python3
"""
Script temporal para probar el producer en contenedor Docker.
Comunicación 1 a 1: Producer -> Queue -> Consumer
"""

import os
import time
import sys
from middleware import RabbitMQMiddlewareQueue

def main():
    print("Iniciando Producer de prueba...")
    
    # Configuración desde variables de entorno
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    queue_name = os.getenv('QUEUE_NAME', 'test_queue_1to1')
    
    print(f"Conectando a RabbitMQ: {rabbitmq_host}")
    print(f"Cola: {queue_name}")
    
    try:
        # Crear middleware
        producer = RabbitMQMiddlewareQueue(rabbitmq_host, queue_name)
        
        # Enviar mensajes de prueba
        messages = [
            {"id": 1, "type": "greeting", "content": "¡Hola desde el producer!", "timestamp": time.time()},
            {"id": 2, "type": "info", "content": "Este es el segundo mensaje", "timestamp": time.time()},
            {"id": 3, "type": "data", "content": "Mensaje con datos complejos", "data": [1, 2, 3, "test"], "timestamp": time.time()},
            {"id": 4, "type": "final", "content": "Último mensaje del producer", "timestamp": time.time()}
        ]
        
        print(f"Enviando {len(messages)} mensajes...")
        
        for i, message in enumerate(messages, 1):
            print(f"  Enviando mensaje {i}: {message['content']}")
            producer.send(message)
            time.sleep(1)  # Pausa entre mensajes
        
        print("Todos los mensajes enviados exitosamente!")
        
        # Cerrar conexión
        producer.close()
        print("Conexión cerrada")
        
    except Exception as e:
        print(f"Error en producer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
