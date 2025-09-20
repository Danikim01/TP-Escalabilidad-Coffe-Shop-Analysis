#!/usr/bin/env python3
"""
Script temporal para probar el consumer en contenedor Docker.
Comunicación 1 a 1: Producer -> Queue -> Consumer
"""

import os
import time
import sys
from middleware import RabbitMQMiddlewareQueue

def message_callback(message):
    """Callback que se ejecuta cuando llega un mensaje."""
    print(f"Mensaje recibido: {message}")
    
    # Simular procesamiento
    if message.get('type') == 'final':
        print("Mensaje final recibido, terminando consumer...")
        return True  # Indicar que queremos terminar
    return False

def main():
    print("Iniciando Consumer de prueba...")
    
    # Configuración desde variables de entorno
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    queue_name = os.getenv('QUEUE_NAME', 'test_queue_1to1')
        
    try:
        # Crear middleware
        consumer = RabbitMQMiddlewareQueue(rabbitmq_host, queue_name)
        
        print("Iniciando escucha de mensajes...")
        print("Esperando mensajes (presiona Ctrl+C para salir)...")
        
        # Variable para controlar el bucle
        should_stop = False
        
        def callback_wrapper(message):
            nonlocal should_stop
            should_stop = message_callback(message)
            return should_stop
        
        # Iniciar consumo en un hilo separado para poder controlarlo
        import threading
        
        def consume_messages():
            try:
                consumer.start_consuming(callback_wrapper)
            except KeyboardInterrupt:
                print("\nInterrupción recibida, deteniendo consumer...")
            except Exception as e:
                print(f"Error en consumer: {e}")
        
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Esperar hasta que se reciba el mensaje final o interrupción
        try:
            while not should_stop:
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\nInterrupción recibida, deteniendo consumer...")
        
        # Detener consumer
        consumer.stop_consuming()
        consumer_thread.join(timeout=5)
        
        # Cerrar conexión
        consumer.close()
        print("Conexión cerrada")
        print("Consumer terminado exitosamente!")
        
    except Exception as e:
        print(f"Error en consumer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
