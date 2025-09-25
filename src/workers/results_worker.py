#!/usr/bin/env python3

import os
import sys
import logging
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResultsWorker:
    """
    Worker final que recibe los resultados procesados y los reenvía al cliente.
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Cola de entrada configurable por entorno
        self.input_queue = os.getenv('INPUT_QUEUE', 'transactions_final_results')

        # Cola de salida para reenviar resultados al cliente
        self.output_queue = os.getenv('OUTPUT_QUEUE', 'client_results')
        
        # Configuración de prefetch para load balancing
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', 10))
        
        # Middleware para recibir datos
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port
        )

        # Middleware para publicar resultados procesados
        self.output_middleware = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.output_queue,
            port=self.rabbitmq_port
        )
        
        # Contador de resultados
        self.result_count = 0
        
        logger.info(
            f"ResultsWorker inicializado - Input: {self.input_queue}, Output: {self.output_queue}"
        )
    
    def process_result(self, result):
        """
        Procesa un resultado individual.
        
        Args:
            result: Diccionario con los datos del resultado
        """
        try:
            self.result_count += 1

            # Reenviar resultado al cliente
            self.output_middleware.send(result)

            transaction_id = result.get('transaction_id', 'unknown')
            final_amount = result.get('final_amount', 0)

            logger.info(f"Resultado #{self.result_count}: {transaction_id} - ${final_amount}")
            
        except Exception as e:
            logger.error(f"Error procesando resultado: {e}")
    
    def process_batch(self, batch):
        """
        Procesa un lote de resultados.
        
        Args:
            batch: Lista de resultados
        """
        try:
            for result in batch:
                self.process_result(result)
            
            logger.info(f"Procesado lote de {len(batch)} resultados")
            
        except Exception as e:
            logger.error(f"Error procesando lote de resultados: {e}")
    
    def start_consuming(self):
        """Inicia el consumo de mensajes de la cola de entrada."""
        try:
            logger.info("Iniciando ResultsWorker...")
            def on_message(message):
                """Callback para procesar mensajes recibidos."""
                try:
                    if isinstance(message, list):
                        # Es un lote de resultados
                        self.process_batch(message)
                    else:
                        # Es un resultado individual
                        self.process_result(message)
                        
                except Exception as e:
                    logger.error(f"Error en callback de mensaje: {e}")
            
            # Iniciar consumo
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            logger.info("Recibida señal de interrupción")
            logger.info(f"Total de resultados encontrados: {self.result_count}")
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
        worker = ResultsWorker()
        worker.start_consuming()
    except Exception as e:
        logger.error(f"Error en main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
