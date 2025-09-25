#!/usr/bin/env python3

import subprocess
import sys
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_worker(worker_name, script_name):
    """Inicia un worker en un proceso separado."""
    try:
        logger.info(f"Iniciando {worker_name}...")
        process = subprocess.Popen([
            sys.executable, script_name
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        logger.info(f"{worker_name} iniciado con PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Error iniciando {worker_name}: {e}")
        return None

def main():
    """Inicia todos los workers."""
    workers = [
        ("Year Filter Worker", "year_filter_worker.py"),
        ("Time Filter Worker", "time_filter_worker.py"),
        ("Amount Filter Worker", "amount_filter_worker.py"),
        ("Results Worker", "results_worker.py")
    ]
    
    processes = []
    
    try:
        # Iniciar todos los workers
        for worker_name, script_name in workers:
            process = start_worker(worker_name, script_name)
            if process:
                processes.append((worker_name, process))
            time.sleep(2)  # Esperar un poco entre workers
        
        logger.info("Todos los workers iniciados. Presiona Ctrl+C para detener.")
        
        # Esperar a que se presione Ctrl+C
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Recibida señal de interrupción")
    
    except Exception as e:
        logger.error(f"Error en main: {e}")
    
    finally:
        # Detener todos los workers
        logger.info("Deteniendo workers...")
        for worker_name, process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
                logger.info(f"{worker_name} detenido")
            except subprocess.TimeoutExpired:
                logger.warning(f"Forzando detención de {worker_name}")
                process.kill()
            except Exception as e:
                logger.error(f"Error deteniendo {worker_name}: {e}")

if __name__ == "__main__":
    main()
