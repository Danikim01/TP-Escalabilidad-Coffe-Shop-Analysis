#!/usr/bin/env python3

import json
import logging

logger = logging.getLogger(__name__)

def serialize_message(message):
    """
    Serializa un mensaje a string JSON.
    
    Args:
        message: Objeto a serializar (dict, list, etc.)
        
    Returns:
        str: Mensaje serializado como JSON
    """
    try:
        return json.dumps(message, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error serializando mensaje: {e}")
        raise

def deserialize_message(serialized_message):
    """
    Deserializa un mensaje desde string JSON.
    
    Args:
        serialized_message: String JSON a deserializar
        
    Returns:
        object: Mensaje deserializado
    """
    try:
        return json.loads(serialized_message)
    except Exception as e:
        logger.error(f"Error deserializando mensaje: {e}")
        raise
