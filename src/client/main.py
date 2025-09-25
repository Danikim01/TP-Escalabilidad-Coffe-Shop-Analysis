import os
import csv
import socket
import logging
import time
import yaml
import sys
from typing import List, Dict, Any
from protocol import DataType, send_batch, send_eof, receive_response

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_input) -> Dict[str, Any]:
    """Load configuration from YAML file or dictionary"""
    try:
        # If it's already a dictionary, return it
        if isinstance(config_input, dict):
            return config_input
        
        # Otherwise, treat it as a file path
        with open(config_input, 'r') as f:
            config = yaml.safe_load(f)
        return config if config else {}
    except FileNotFoundError:
        logger.warning(f"Config file {config_input} not found, using defaults")
        return {}
    except Exception as e:
        logger.error(f"Error reading config file {config_input}: {e}")
        return {}

# Maximum serialized size per row type in bytes (calculated from CSV structure analysis)
MAX_ROW_SIZES = {
    DataType.TRANSACTIONS: 95,      # transaction_id(40) + store_id(4) + payment_method_id(4) + 
                                    # voucher_id(4) + user_id(4) + amounts(12) + created_at(23) + overhead(4)
    DataType.TRANSACTION_ITEMS: 83,  # transaction_id(40) + item_id(4) + quantity(4) + 
                                     # unit_price(4) + subtotal(4) + created_at(23) + overhead(4)
    DataType.USERS: 56              # user_id(4) + gender(11) + birthdate(14) + 
                                    # registered_at(23) + overhead(4)
}

def get_max_row_size(data_type: DataType) -> int:
    """Get the maximum serialized size for a row of the given data type"""
    return MAX_ROW_SIZES.get(data_type, 100)  # Default to 100 if unknown type

def estimate_row_size(data_type: DataType, sample_row: Dict[str, Any] = None) -> int:
    """Estimate the serialized size of a row in bytes using hardcoded maximums"""
    # Use hardcoded maximum sizes instead of actual serialization
    # This is more efficient and the sizes are predictable based on CSV structure
    return get_max_row_size(data_type)

class CoffeeShopClient:
    def __init__(self, config_input='config.yaml'):
        # Load configuration
        self.config = load_config(config_input)
        
        # Gateway configuration with environment variable fallback
        gateway_config = self.config.get('gateway', {})
        self.gateway_host = os.getenv('GATEWAY_HOST', gateway_config.get('host', 'localhost'))
        self.gateway_port = int(os.getenv('GATEWAY_PORT', gateway_config.get('port', 12345)))
        
        # Batch configuration
        batch_config = self.config.get('batch', {})
        self.max_batch_size_kb = batch_config.get('max_size_kb', 64)
        
        # Logging configuration
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        self.data_dir = '.data'
        self.socket = None
        
        logger.info(f"Client configured - Gateway: {self.gateway_host}:{self.gateway_port}, "
                   f"Batch: {self.max_batch_size_kb}KB max")
        
    def connect_to_gateway(self):
        """Establish connection to gateway"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.gateway_host, self.gateway_port))
            logger.info(f"Connected to gateway at {self.gateway_host}:{self.gateway_port}")
        except Exception as e:
            logger.error(f"Failed to connect to gateway: {e}")
            raise
            
    def disconnect(self):
        """Close connection to gateway"""
        if self.socket:
            self.socket.close()
            self.socket = None
            logger.info("Disconnected from gateway")
    
    def get_csv_files_by_type(self, data_type_str: str) -> List[str]:
        """Get all CSV files for a specific data type"""
        type_dir = os.path.join(self.data_dir, data_type_str)
        
        if not os.path.exists(type_dir):
            logger.warning(f"Directory {type_dir} does not exist")
            return []
            
        csv_files = []
        for file in os.listdir(type_dir):
            if file.endswith('.csv'):
                csv_files.append(os.path.join(type_dir, file))
        
        csv_files.sort()  # Process files in order
        logger.info(f"Found {len(csv_files)} CSV files in {type_dir}")
        return csv_files
    
    def process_csv_file_streaming(self, file_path: str, data_type: DataType, batch_size: int):
        """Stream process CSV file, sending batches as they're read"""
        total_rows = 0
        current_batch = []
        
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    # Clean up the row data
                    cleaned_row = {}
                    for key, value in row.items():
                        cleaned_key = key.strip()
                        cleaned_value = value.strip() if isinstance(value, str) else value
                        cleaned_row[cleaned_key] = cleaned_value
                    
                    current_batch.append(cleaned_row)
                    
                    # Send batch when it reaches the target size
                    if len(current_batch) >= batch_size:
                        if not self._send_batch_to_gateway(current_batch, data_type):
                            return total_rows  # Return on error
                        total_rows += len(current_batch)
                        current_batch = []
                
                # Send remaining rows in the last batch
                if current_batch:
                    if not self._send_batch_to_gateway(current_batch, data_type):
                        return total_rows  # Return on error
                    total_rows += len(current_batch)
            
            logger.info(f"Streamed {total_rows} rows from {file_path}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Failed to stream {file_path}: {e}")
            return total_rows
    
    def _send_batch_to_gateway(self, batch, data_type: DataType) -> bool:
        """Send a batch to the gateway and handle response"""
        try:
            send_batch(self.socket, data_type, batch)
            
            # Wait for response
            response_code = receive_response(self.socket)
            if response_code == 0:  # ResponseCode.OK
                logger.debug(f"Successfully sent batch of {len(batch)} rows for {data_type.name}")
                return True
            else:
                logger.error(f"Gateway rejected batch for {data_type.name} (response code: {response_code})")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send batch for {data_type.name}: {e}")
            return False
    
    def calculate_optimal_batch_size(self, data_type: DataType) -> int:
        """Calculate optimal batch size based on KB limit and hardcoded row size"""
        
        max_row_size = get_max_row_size(data_type)
        max_batch_size_bytes = self.max_batch_size_kb * 1024
        
        # Calculate how many rows fit in the KB limit
        optimal_batch_size = max(1, int(max_batch_size_bytes / max_row_size))
        
        # Cap at reasonable maximum to avoid memory issues
        optimal_batch_size = min(optimal_batch_size, 10000)
        
        logger.info(f"Calculated batch size for {data_type.name}: {optimal_batch_size} rows "
                   f"(max row size: {max_row_size} bytes, target: {self.max_batch_size_kb}KB)")
        
        return optimal_batch_size
    
    def send_data_type_files(self, data_type: DataType, data_type_str: str):
        """Send all files for a specific data type using streaming approach"""
        csv_files = self.get_csv_files_by_type(data_type_str)
        
        if not csv_files:
            logger.info(f"No CSV files found for {data_type_str}")
            send_eof(self.socket, data_type)
            return
        
        total_rows = 0
        batch_size = self.calculate_optimal_batch_size(data_type)
        
        for csv_file in csv_files:
            logger.info(f"Streaming file: {csv_file}")
            
            # Stream process the file
            file_rows = self.process_csv_file_streaming(csv_file, data_type, batch_size)
            
            if file_rows == 0:
                logger.warning(f"No rows processed from {csv_file}")
                continue
            
            total_rows += file_rows
            logger.info(f"Completed streaming {csv_file}: {file_rows} rows sent")
        
        logger.info(f"Finished streaming {total_rows} total rows for {data_type_str} "
                   f"(batch size: {batch_size} rows)")
        
        # Send EOF for this data type
        try:
            send_eof(self.socket, data_type)
            logger.info(f"Sent EOF for {data_type_str}")
        except Exception as e:
            logger.error(f"Failed to send EOF for {data_type_str}: {e}")
    
    def run(self):
        """Main client execution"""
        try:
            self.connect_to_gateway()
            
            # Send data for each type in order
            data_types = [
                (DataType.USERS, 'users'),
                (DataType.TRANSACTIONS, 'transactions'), 
                (DataType.TRANSACTION_ITEMS, 'transaction_items')
            ]
            
            for data_type, data_type_str in data_types:
                logger.info(f"Starting to send {data_type_str} data")
                self.send_data_type_files(data_type, data_type_str)
            
            logger.info("All data sent successfully")
            
        except Exception as e:
            logger.error(f"Error in client execution: {e}")
        finally:
            self.disconnect()

def main():
    """Entry point"""
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'
    client = CoffeeShopClient(config_file)
    client.run()

if __name__ == "__main__":
    main()