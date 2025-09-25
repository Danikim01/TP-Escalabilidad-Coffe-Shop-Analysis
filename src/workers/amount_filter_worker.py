#!/usr/bin/env python3

import os
import sys
import logging
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AmountFilterWorker:
    """
    Worker que filtra transacciones por monto (>= 75).
    Recibe transacciones filtradas por año y hora, y las filtra por monto.
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Colas de entrada y salida
        self.input_queue = 'transactions_time_filtered'
        self.output_queue = 'transactions_final_results'
        
        # Configuración de prefetch para load balancing
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))
        
        # Middleware para recibir datos con prefetch optimizado
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count
        )
        
        # Middleware para enviar datos filtrados
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port
        )
        
        # Monto mínimo requerido
        self.min_amount = 75.0
        
        # Worker inicializado sin logs para optimización
        logger.info(f"Filtro de monto: >= {self.min_amount}")
    
    def filter_by_amount(self, transaction):
        """
        Filtra una transacción por monto (>= 75).
        
        Args:
            transaction: Diccionario con los datos de la transacción
            
        Returns:
            bool: True si la transacción cumple el filtro de monto
        """
        try:
            # Obtener el monto final de la transacción
            # Según el schema, tenemos original_amount, discount_applied y final_amount
            # Usaremos final_amount como el monto total
            final_amount = transaction.get('final_amount')
            
            if final_amount is None:
                # Si no hay final_amount, intentar calcularlo
                original_amount = float(transaction.get('original_amount', 0))
                discount_applied = float(transaction.get('discount_applied', 0))
                final_amount = original_amount - discount_applied
            else:
                final_amount = float(final_amount)
            
            # Verificar si cumple el monto mínimo
            return final_amount >= self.min_amount
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parseando monto de transacción: {e}")
            return False
        except Exception:
            return False
    
    def process_transaction(self, transaction):
        """
        Procesa una transacción individual.
        
        Args:
            transaction: Diccionario con los datos de la transacción
        """
        try:
            # Aplicar filtro de monto
            if self.filter_by_amount(transaction):
                # Crear resultado con solo ID y monto como se solicita
                result = {
                    'transaction_id': transaction.get('transaction_id'),
                    'final_amount': transaction.get('final_amount'),
                    'original_amount': transaction.get('original_amount'),
                    'discount_applied': transaction.get('discount_applied'),
                    'created_at': transaction.get('created_at')
                }
                
                # Enviar resultado al final
                self.output_middleware.send(result)
                pass
            else:
                pass
                
        except Exception:
            pass
    
    def process_batch(self, batch):
        """
        Procesa un lote de transacciones (puede ser chunk o transacciones individuales).
        Procesa y envía inmediatamente sin almacenar en memoria.
        
        Args:
            batch: Lista de transacciones o chunk de transacciones
        """
        try:
            # Procesar cada transacción individualmente y enviar inmediatamente
            # Sin almacenar en memoria (cumple restricción de cátedra)
            for transaction in batch:
                if self.filter_by_amount(transaction):
                    # Crear resultado con solo ID y monto y enviar inmediatamente
                    result = {
                        'transaction_id': transaction.get('transaction_id'),
                        'final_amount': transaction.get('final_amount'),
                        'original_amount': transaction.get('original_amount'),
                        'discount_applied': transaction.get('discount_applied'),
                        'created_at': transaction.get('created_at')
                    }
                    self.output_middleware.send(result)
            
        except Exception:
            pass
    
    def start_consuming(self):
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            # Iniciando worker sin logs para optimización
            
            def on_message(message):
                """Callback para procesar mensajes recibidos."""
                try:
                    if isinstance(message, list):
                        # Es un lote de transacciones
                        self.process_batch(message)
                    else:
                        # Es una transacción individual
                        self.process_transaction(message)
                        
                except Exception:
                    pass
            
            # Iniciar consumo
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos."""
        try:
            self.input_middleware.close()
            self.output_middleware.close()
            logger.info("Recursos limpiados")
        except Exception as e:
            logger.warning(f"Error limpiando recursos: {e}")

def main():
    """Punto de entrada principal."""
    try:
        worker = AmountFilterWorker()
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error en main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
