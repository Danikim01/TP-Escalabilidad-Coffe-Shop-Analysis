from typing import Dict, Any

def serialize_message(message: Any) -> str:
    """
    Serializa un mensaje a string sin usar librerías externas.
    Soporta dict, list, str, int, float, bool, None.
    """
    if message is None:
        return "null"
    elif isinstance(message, bool):
        return "true" if message else "false"
    elif isinstance(message, (int, float)):
        return str(message)
    elif isinstance(message, str):
        # Escapar comillas y caracteres especiales
        escaped = message.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
        return f'"{escaped}"'
    elif isinstance(message, list):
        items = [serialize_message(item) for item in message]
        return f"[{','.join(items)}]"
    elif isinstance(message, dict):
        pairs = []
        for key, value in message.items():
            if not isinstance(key, str):
                raise ValueError(f"Las claves del diccionario deben ser strings, recibido: {type(key)}")
            serialized_key = serialize_message(key)
            serialized_value = serialize_message(value)
            pairs.append(f"{serialized_key}:{serialized_value}")
        return f"{{{','.join(pairs)}}}"
    else:
        raise ValueError(f"Tipo no soportado para serialización: {type(message)}")


def deserialize_message(serialized: str) -> Any:
    """
    Deserializa un string a objeto Python.
    """
    serialized = serialized.strip()
    
    if serialized == "null":
        return None
    elif serialized == "true":
        return True
    elif serialized == "false":
        return False
    elif serialized.startswith('"') and serialized.endswith('"'):
        # String
        content = serialized[1:-1]
        # Desescapar
        content = content.replace("\\r", "\r").replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")
        return content
    elif serialized.startswith('[') and serialized.endswith(']'):
        # Lista
        content = serialized[1:-1].strip()
        if not content:
            return []
        
        items = []
        current_item = ""
        depth = 0
        in_string = False
        escape_next = False
        
        for char in content:
            if escape_next:
                current_item += char
                escape_next = False
                continue
                
            if char == '\\':
                escape_next = True
                current_item += char
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                current_item += char
                continue
                
            if not in_string:
                if char == '[' or char == '{':
                    depth += 1
                elif char == ']' or char == '}':
                    depth -= 1
                elif char == ',' and depth == 0:
                    items.append(deserialize_message(current_item.strip()))
                    current_item = ""
                    continue
            
            current_item += char
        
        if current_item.strip():
            items.append(deserialize_message(current_item.strip()))
        
        return items
    elif serialized.startswith('{') and serialized.endswith('}'):
        # Diccionario
        content = serialized[1:-1].strip()
        if not content:
            return {}
        
        result = {}
        current_pair = ""
        depth = 0
        in_string = False
        escape_next = False
        
        for char in content:
            if escape_next:
                current_pair += char
                escape_next = False
                continue
                
            if char == '\\':
                escape_next = True
                current_pair += char
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                current_pair += char
                continue
                
            if not in_string:
                if char == '[' or char == '{':
                    depth += 1
                elif char == ']' or char == '}':
                    depth -= 1
                elif char == ',' and depth == 0:
                    key, value = current_pair.split(':', 1)
                    result[deserialize_message(key.strip())] = deserialize_message(value.strip())
                    current_pair = ""
                    continue
            
            current_pair += char
        
        if current_pair.strip():
            key, value = current_pair.split(':', 1)
            result[deserialize_message(key.strip())] = deserialize_message(value.strip())
        
        return result
    else:
        # Número
        try:
            if '.' in serialized:
                return float(serialized)
            else:
                return int(serialized)
        except ValueError:
            raise ValueError(f"No se pudo deserializar: {serialized}")