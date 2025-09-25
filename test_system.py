#!/usr/bin/env python3

import time
import logging
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_transaction():
    """Envía una transacción de prueba al sistema."""
    
    # Transacción de prueba que debería pasar todos los filtros
    test_transaction = {
        'transaction_id': 'test-123-456-789',
        'store_id': 1,
        'payment_method_id': 1,
        'voucher_id': '',
        'user_id': 1,
        'original_amount': 100.0,
        'discount_applied': 10.0,
        'final_amount': 90.0,
        'created_at': '2024-07-15 10:30:00'  # 2024, entre 06:00 y 23:00, monto >= 75
    }
    
    try:
        # Conectar a RabbitMQ
        middleware = RabbitMQMiddlewareQueue(
            host='localhost',
            queue_name='transactions_raw',
            port=5672
        )
        
        # Enviar transacción de prueba
        middleware.send(test_transaction)
        logger.info("Transacción de prueba enviada")
        
        # Cerrar conexión
        middleware.close()
        
    except Exception as e:
        logger.error(f"Error enviando transacción de prueba: {e}")

def main():
    """Función principal de prueba."""
    logger.info("Iniciando prueba del sistema...")
    
    # Esperar un poco para que los workers se inicialicen
    logger.info("Esperando 5 segundos para que los workers se inicialicen...")
    time.sleep(5)
    
    # Enviar transacción de prueba
    test_transaction()
    
    logger.info("Prueba completada. Revisa los logs de los workers para ver los resultados.")

if __name__ == "__main__":
    main()
