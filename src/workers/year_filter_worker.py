#!/usr/bin/env python3

import os
import sys
import logging
from datetime import datetime
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YearFilterWorker:
    """
    Worker que filtra transacciones por año (2024 y 2025).
    Recibe transacciones y las filtra según el año en created_at.
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Colas de entrada y salida
        self.input_queue = 'transactions_raw'
        self.output_queue = 'transactions_year_filtered'
        
        # Configuración de prefetch para load balancing
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))
        
        # Middleware para recibir datos
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port
        )
        
        # Middleware para enviar datos filtrados
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port
        )
        
        # Worker inicializado sin logs para optimización
    
    def filter_by_year(self, transaction):
        """
        Filtra una transacción por año (2024 y 2025).
        
        Args:
            transaction: Diccionario con los datos de la transacción
            
        Returns:
            bool: True si la transacción cumple el filtro de año
        """
        try:
            # Extraer la fecha de created_at
            created_at = transaction.get('created_at', '')
            if not created_at:
                return False
            
            # Parsear la fecha (formato esperado: YYYY-MM-DD HH:MM:SS)
            date_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            year = date_obj.year
            
            # Filtrar por años 2024 y 2025
            return year in [2024, 2025]
            
        except ValueError:
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
            # Aplicar filtro de año
            if self.filter_by_year(transaction):
                # Enviar transacción filtrada al siguiente worker
                self.output_middleware.send(transaction)
                
        except Exception:
            pass
    
    def process_batch(self, batch):
        """
        Procesa un lote de transacciones.
        
        Args:
            batch: Lista de transacciones
        """
        try:
            for transaction in batch:
                if self.filter_by_year(transaction):
                    self.output_middleware.send(transaction)
            
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
        worker = YearFilterWorker()
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error en main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
