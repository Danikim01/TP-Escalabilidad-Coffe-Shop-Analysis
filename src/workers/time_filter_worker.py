#!/usr/bin/env python3

import os
import sys
import logging
from typing import Any
from datetime import datetime, time
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimeFilterWorker:
    """
    Worker que filtra transacciones por hora (06:00 AM - 11:00 PM).
    Recibe transacciones filtradas por año y las filtra por hora.
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Colas de entrada y salida configurables por entorno
        self.input_queue = os.getenv('INPUT_QUEUE', 'transactions_year_filtered')
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'transactions_time_filtered')
        
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

    
        # Definir rango de horas (06:00 AM - 11:00 PM)
        self.start_time = time(6, 0)   # 06:00 AM
        self.end_time = time(23, 0)    # 11:00 PM (23:00)
        
        # Worker inicializado sin logs para optimización
        # Filtro de hora configurado
    
    def filter_by_time(self, transaction):
        """
        Filtra una transacción por hora (06:00 AM - 11:00 PM).
        
        Args:
            transaction: Diccionario con los datos de la transacción
            
        Returns:
            bool: True si la transacción cumple el filtro de hora
        """
        try:
            # Extraer la fecha y hora de created_at
            created_at = transaction.get('created_at', '')
            if not created_at:
                return False
            
            # Parsear la fecha y hora (formato esperado: YYYY-MM-DD HH:MM:SS)
            datetime_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            transaction_time = datetime_obj.time()
            
            # Verificar si está en el rango de horas
            # Considerar que 11:00 PM es 23:00, así que el rango es 06:00-23:00
            return self.start_time <= transaction_time <= self.end_time
            
        except ValueError:
            return False
        except Exception:
            return False
    
    def _is_eof(self, message: Any) -> bool:
        return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'

    def process_transaction(self, transaction):
        """
        Procesa una transacción individual.
        
        Args:
            transaction: Diccionario con los datos de la transacción
        """
        try:
            # Aplicar filtro de hora
            if self.filter_by_time(transaction):
                # Enviar transacción filtrada al siguiente worker
                self.output_middleware.send(transaction)
                pass
            else:
                pass
                
        except Exception:
            pass
    
    def process_batch(self, batch):
        """
        Procesa un lote de transacciones.
        
        Args:
            batch: Lista de transacciones
        """
        try:
            filtered_count = 0
            total_count = len(batch)
            
            for transaction in batch:
                if self.filter_by_time(transaction):
                    self.output_middleware.send(transaction)
                    filtered_count += 1
            
            # Lote procesado sin logs
            
        except Exception:
            pass
    
    def start_consuming(self):
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            # Iniciando worker sin logs para optimización
            
            def on_message(message):
                """Callback para procesar mensajes recibidos."""
                try:
                    if self._is_eof(message):
                        self.output_middleware.send({'type': 'EOF'})
                        self.input_middleware.stop_consuming()
                        return

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
        worker = TimeFilterWorker()
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error en main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
