#!/usr/bin/env python3

import os
import sys
import logging
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
        
        # Colas de entrada y salida
        self.input_queue = 'transactions_year_filtered'
        self.output_queue = 'transactions_time_filtered'
        
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
        
        logger.info(f"TimeFilterWorker inicializado - Input: {self.input_queue}, Output: {self.output_queue}")
        logger.info(f"Filtro de hora: {self.start_time} - {self.end_time}")
    
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
            
        except ValueError as e:
            logger.warning(f"Error parseando fecha/hora '{created_at}': {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado filtrando transacción: {e}")
            return False
    
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
                logger.debug(f"Transacción {transaction.get('transaction_id', 'unknown')} pasó filtro de hora")
            else:
                logger.debug(f"Transacción {transaction.get('transaction_id', 'unknown')} no pasó filtro de hora")
                
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
    
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
            
            logger.info(f"Procesado lote: {filtered_count}/{total_count} transacciones pasaron filtro de hora")
            
        except Exception as e:
            logger.error(f"Error procesando lote: {e}")
    
    def start_consuming(self):
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            logger.info("Iniciando TimeFilterWorker...")
            
            def on_message(message):
                """Callback para procesar mensajes recibidos."""
                try:
                    if isinstance(message, list):
                        # Es un lote de transacciones
                        self.process_batch(message)
                    else:
                        # Es una transacción individual
                        self.process_transaction(message)
                        
                except Exception as e:
                    logger.error(f"Error en callback de mensaje: {e}")
            
            # Iniciar consumo
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            logger.info("Recibida señal de interrupción")
        except Exception as e:
            logger.error(f"Error iniciando consumo: {e}")
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
