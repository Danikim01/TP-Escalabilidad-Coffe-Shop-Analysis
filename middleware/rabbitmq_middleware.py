#!/usr/bin/env python3

import pika
import logging
from typing import Callable, List, Optional
from .middleware_interface import (
    MessageMiddleware, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError
)
from protocol import serialize_message, deserialize_message

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQMiddlewareQueue(MessageMiddleware):
    """
    Implementación de MessageMiddleware para comunicación por colas (Working Queue).
    Soporta comunicación 1 a 1 y 1 a N (competing consumers pattern).
    """
    
    def __init__(self, host: str, queue_name: str, port: int = 5672):
        """
        Inicializa el middleware para comunicación por cola.
        
        Args:
            host: Host de RabbitMQ
            queue_name: Nombre de la cola
            port: Puerto de RabbitMQ (por defecto 5672)
        """
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.consuming = False
        
        logger.info(f"Inicializando RabbitMQ Queue Middleware: {host}:{port}/{queue_name}")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
                logger.info(f"Conectado a RabbitMQ: {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Error conectando a RabbitMQ: {e}")
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """
        Comienza a escuchar la cola e invoca on_message_callback para cada mensaje.
        
        Args:
            on_message_callback: Función que se ejecutará para cada mensaje recibido
        """
        try:
            self._ensure_connection()
            
            # Declarar la cola (es idempotente)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            
            # Configurar el consumidor
            def callback(ch, method, properties, body):
                try:
                    # Decodificar el mensaje
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    logger.info(f"Mensaje recibido en cola '{self.queue_name}': {message}")
                    on_message_callback(message)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except ValueError as e:
                    logger.error(f"Error deserializando mensaje: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    raise MessageMiddlewareMessageError(f"Error deserializando mensaje: {e}")
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")
            
            # Configurar QoS para distribuir mensajes equitativamente entre consumidores
            self.channel.basic_qos(prefetch_count=1)
            
            # Configurar el consumidor
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False  # Manual ack para garantizar procesamiento
            )
            
            self.consuming = True
            logger.info(f"Iniciando consumo de la cola '{self.queue_name}'")
            
            # Comenzar a consumir mensajes
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            logger.error(f"Error iniciando consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def stop_consuming(self):
        """Detiene la escucha de la cola."""
        try:
            if self.channel and self.consuming:
                self.channel.stop_consuming()
                self.consuming = False
                logger.info(f"Detenido consumo de la cola '{self.queue_name}'")
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Error de conexión AMQP al detener consumo (normal al cerrar): {e}")
            # No lanzar excepción en el cierre, es normal que falle
        except Exception as e:
            logger.warning(f"Error deteniendo consumo (normal al cerrar): {e}")
            # No lanzar excepción en el cierre, es normal que falle
    
    def send(self, message):
        """
        Envía un mensaje a la cola.
        
        Args:
            message: Mensaje a enviar (será serializado manualmente)
        """
        try:
            self._ensure_connection()
            
            # Declarar la cola (es idempotente)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            
            # Serializar el mensaje manualmente
            message_body = serialize_message(message)
            
            # Enviar el mensaje
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                )
            )
            
            logger.info(f"Mensaje enviado a la cola '{self.queue_name}': {message}")
            
        except MessageMiddlewareDisconnectedError:
            # Re-lanzar errores de conexión sin modificar
            raise
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP al enviar: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            raise MessageMiddlewareMessageError(f"Error interno enviando mensaje: {e}")
    
    def close(self):
        """Se desconecta de RabbitMQ."""
        try:
            if self.consuming:
                self.stop_consuming()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info(f"Desconectado de RabbitMQ: {self.host}:{self.port}")
                
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            raise MessageMiddlewareCloseError(f"Error interno cerrando conexión: {e}")
    
    def delete(self):
        """Elimina la cola de RabbitMQ."""
        try:
            self._ensure_connection()
            self.channel.queue_delete(queue=self.queue_name)
            logger.info(f"Cola '{self.queue_name}' eliminada")
        except Exception as e:
            logger.error(f"Error eliminando cola: {e}")
            raise MessageMiddlewareDeleteError(f"Error interno eliminando cola: {e}")


class RabbitMQMiddlewareExchange(MessageMiddleware):
    """
    Implementación de MessageMiddleware para comunicación por exchange.
    Soporta comunicación 1 a 1 y 1 a N usando routing keys.
    """
    
    def __init__(self, host: str, exchange_name: str, route_keys: List[str], 
                 exchange_type: str = 'direct', port: int = 5672):
        """
        Inicializa el middleware para comunicación por exchange.
        
        Args:
            host: Host de RabbitMQ
            exchange_name: Nombre del exchange
            route_keys: Lista de routing keys para escuchar
            exchange_type: Tipo de exchange ('direct', 'fanout', 'topic')
            port: Puerto de RabbitMQ (por defecto 5672)
        """
        self.host = host
        self.port = port
        self.exchange_name = exchange_name
        self.route_keys = route_keys
        self.exchange_type = exchange_type
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.consuming = False
        self.queue_name: Optional[str] = None
        
        logger.info(f"Inicializando RabbitMQ Exchange Middleware: {host}:{port}/{exchange_name} ({exchange_type})")
    
    def _ensure_connection(self):
        """Asegura que la conexión esté establecida."""
        if self.connection is None or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
                logger.info(f"Conectado a RabbitMQ: {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Error conectando a RabbitMQ: {e}")
                raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ: {e}")
    
    def start_consuming(self, on_message_callback: Callable):
        """
        Comienza a escuchar el exchange e invoca on_message_callback para cada mensaje.
        
        Args:
            on_message_callback: Función que se ejecutará para cada mensaje recibido
        """
        try:
            self._ensure_connection()
            
            # Declarar el exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name, 
                exchange_type=self.exchange_type,
                durable=True
            )
            
            # Crear una cola temporal exclusiva para este consumidor
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue
            
            # Bindear la cola a cada routing key
            for route_key in self.route_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=route_key
                )
                logger.info(f"Cola '{self.queue_name}' bindeada a '{route_key}'")
            
            # Configurar el consumidor
            def callback(ch, method, properties, body):
                try:
                    # Decodificar el mensaje
                    serialized_message = body.decode('utf-8')
                    message = deserialize_message(serialized_message)
                    routing_key = method.routing_key
                    logger.info(f"Mensaje recibido en exchange '{self.exchange_name}' con routing key '{routing_key}': {message}")
                    on_message_callback(message)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except ValueError as e:
                    logger.error(f"Error deserializando mensaje: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    raise MessageMiddlewareMessageError(f"Error deserializando mensaje: {e}")
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")
            
            # Configurar QoS
            self.channel.basic_qos(prefetch_count=1)
            
            # Configurar el consumidor
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
            
            self.consuming = True
            logger.info(f"Iniciando consumo del exchange '{self.exchange_name}' con routing keys: {self.route_keys}")
            
            # Comenzar a consumir mensajes
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            logger.error(f"Error iniciando consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error interno iniciando consumo: {e}")
    
    def stop_consuming(self):
        """Detiene la escucha del exchange."""
        try:
            if self.channel and self.consuming:
                self.channel.stop_consuming()
                self.consuming = False
                logger.info(f"Detenido consumo del exchange '{self.exchange_name}'")
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Error de conexión AMQP al detener consumo (normal al cerrar): {e}")
            # No lanzar excepción en el cierre, es normal que falle
        except Exception as e:
            logger.warning(f"Error deteniendo consumo (normal al cerrar): {e}")
            # No lanzar excepción en el cierre, es normal que falle
    
    def send(self, message, routing_key: str = None):
        """
        Envía un mensaje al exchange.
        
        Args:
            message: Mensaje a enviar (será serializado manualmente)
            routing_key: Routing key para el mensaje (opcional, usa la primera si no se especifica)
        """
        try:
            self._ensure_connection()
            
            # Declarar el exchange
            self.channel.exchange_declare(
                exchange=self.exchange_name, 
                exchange_type=self.exchange_type,
                durable=True
            )
            
            # Usar la primera routing key si no se especifica una
            if routing_key is None:
                routing_key = self.route_keys[0] if self.route_keys else ''
            
            # Serializar el mensaje manualmente
            message_body = serialize_message(message)
            
            # Enviar el mensaje
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                )
            )
            
            logger.info(f"Mensaje enviado al exchange '{self.exchange_name}' con routing key '{routing_key}': {message}")
            
        except MessageMiddlewareDisconnectedError:
            # Re-lanzar errores de conexión sin modificar
            raise
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP al enviar: {e}")
            raise MessageMiddlewareDisconnectedError(f"Pérdida de conexión con RabbitMQ: {e}")
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            raise MessageMiddlewareMessageError(f"Error interno enviando mensaje: {e}")
    
    def close(self):
        """Se desconecta de RabbitMQ."""
        try:
            if self.consuming:
                self.stop_consuming()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info(f"Desconectado de RabbitMQ: {self.host}:{self.port}")
                
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            raise MessageMiddlewareCloseError(f"Error interno cerrando conexión: {e}")
    
    def delete(self):
        """Elimina el exchange de RabbitMQ."""
        try:
            self._ensure_connection()
            self.channel.exchange_delete(exchange=self.exchange_name)
            logger.info(f"Exchange '{self.exchange_name}' eliminado")
        except Exception as e:
            logger.error(f"Error eliminando exchange: {e}")
            raise MessageMiddlewareDeleteError(f"Error interno eliminando exchange: {e}")
