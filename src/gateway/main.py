import socket
import logging
import threading
import os
from typing import Dict, Set
from protocol import (
    MessageType, DataType, send_response, receive_message, 
    parse_batch_message, parse_eof_message
)
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoffeeShopGateway:
    def __init__(self, port=12345):
        self.port = port
        self.socket = None
        self.running = False
        self.data_storage = {
            DataType.USERS: [],
            DataType.TRANSACTIONS: [],
            DataType.TRANSACTION_ITEMS: []
        }
        self.eof_received = {
            DataType.USERS: False,
            DataType.TRANSACTIONS: False,
            DataType.TRANSACTION_ITEMS: False
        }
        
        # Configurar RabbitMQ para enviar transacciones a workers
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Middleware para enviar transacciones a la cola de procesamiento
        self.transactions_queue = RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name='transactions_raw',
            port=self.rabbitmq_port
        )
        
        logger.info(f"Gateway configurado con RabbitMQ: {self.rabbitmq_host}:{self.rabbitmq_port}")
    
    def start_server(self):
        """Start the gateway server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(('0.0.0.0', self.port))  # Listen on all interfaces for Docker
            self.socket.listen(5)
            self.running = True
            
            logger.info(f"Gateway server started on port {self.port}")
            
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    logger.info(f"New client connected from {address}")
                    
                    # Handle client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
        finally:
            self.stop_server()
    
    def stop_server(self):
        """Stop the gateway server"""
        self.running = False
        if self.socket:
            self.socket.close()
            logger.info("Gateway server stopped")
    
    def handle_client(self, client_socket: socket.socket, address):
        """Handle a client connection"""
        try:
            while self.running:
                try:
                    # Receive message
                    message_type, message_data = receive_message(client_socket)
                    logger.debug(f"Received message type {message_type}, data size {len(message_data)}")
                    
                    if message_type == MessageType.BATCH:
                        self.handle_batch_message(client_socket, message_data)
                        
                    elif message_type == MessageType.EOF:
                        self.handle_eof_message(client_socket, message_data)
                            
                    else:
                        logger.warning(f"Unknown message type: {message_type}")
                        send_response(client_socket, False)
                        
                except ConnectionError:
                    logger.info(f"Client {address} disconnected")
                    break
                except Exception as e:
                    logger.error(f"Error handling message from {address}: {e}")
                    try:
                        send_response(client_socket, False)
                    except:
                        pass
                    break
                    
        except Exception as e:
            logger.error(f"Error in client handler for {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Connection with {address} closed")
    
    def handle_batch_message(self, client_socket: socket.socket, message_data: bytes):
        """Handle a batch of data rows"""
        try:
            data_type, rows = parse_batch_message(message_data)
            
            logger.info(f"Received batch: type={data_type.name}, size={len(rows)}")
            
            # Si son transacciones, enviarlas a la cola de procesamiento
            if data_type == DataType.TRANSACTIONS:
                logger.info(f"Enviando {len(rows)} transacciones a la cola de procesamiento")
                try:
                    # Enviar cada transacción individualmente a la cola
                    for transaction in rows:
                        self.transactions_queue.send(transaction)
                    logger.info(f"Enviadas {len(rows)} transacciones a la cola de procesamiento")
                except Exception as e:
                    logger.error(f"Error enviando transacciones a RabbitMQ: {e}")
            
            # Send success response
            send_response(client_socket, True)
            
        except Exception as e:
            logger.error(f"Failed to process batch message: {e}")
            send_response(client_socket, False)
    
    def handle_eof_message(self, client_socket: socket.socket, message_data: bytes):
        """Handle EOF message for a data type"""
        try:
            data_type = parse_eof_message(message_data)
            
            if not self.eof_received[data_type]:
                self.eof_received[data_type] = True
                logger.info(f"Received EOF for {data_type.name}.")
            else:
                logger.warning(f"Duplicate EOF received for {data_type.name}")
            
            # Send success response
            send_response(client_socket, True)
            
        except Exception as e:
            logger.error(f"Failed to process EOF message: {e}")
            send_response(client_socket, False)
    
def main():
    """Entry point"""
    gateway = CoffeeShopGateway()
    
    try:
        gateway.start_server()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        gateway.stop_server()

if __name__ == "__main__":
    main()