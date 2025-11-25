"""
Control de Carga Incremental para ETL
Maneja las fechas de última extracción para evitar duplicados
"""

import json
import os
from datetime import datetime
from typing import Optional

class IncrementalControl:
    """Clase para manejar control incremental"""
    
    def __init__(self, control_file_path: str = "logs/incremental_control.json"):
        self.control_file = control_file_path
        self._ensure_control_file_exists()
    
    def _ensure_control_file_exists(self):
        """Crear archivo de control si no existe"""
        if not os.path.exists(self.control_file):
            os.makedirs(os.path.dirname(self.control_file), exist_ok=True)
            initial_data = {
                "last_extraction": "1900-01-01 00:00:00",
                "extractions_history": []
            }
            with open(self.control_file, 'w') as f:
                json.dump(initial_data, f, indent=2)
    
    def get_last_extraction_date(self) -> str:
        """Obtener fecha de última extracción"""
        try:
            with open(self.control_file, 'r') as f:
                data = json.load(f)
                return data.get("last_extraction", "1900-01-01 00:00:00")
        except Exception:
            return "1900-01-01 00:00:00"
    
    def update_last_extraction_date(self, extraction_date: Optional[str] = None):
        """Actualizar fecha de última extracción"""
        if extraction_date is None:
            extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            with open(self.control_file, 'r') as f:
                data = json.load(f)
        except Exception:
            data = {"extractions_history": []}
        
        # Actualizar fecha
        old_date = data.get("last_extraction", "1900-01-01 00:00:00")
        data["last_extraction"] = extraction_date
        
        # Guardar en historial
        data["extractions_history"].append({
            "previous_date": old_date,
            "new_date": extraction_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Mantener solo últimas 10 ejecuciones
        data["extractions_history"] = data["extractions_history"][-10:]
        
        with open(self.control_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_incremental_filter(self, date_column: str = "fecha_modificacion") -> str:
        """Generar filtro SQL para carga incremental"""
        last_date = self.get_last_extraction_date()
        return f"{date_column} > '{last_date}'"
    
    def reset_control(self):
        """Resetear control (para carga completa)"""
        self.update_last_extraction_date("1900-01-01 00:00:00")
        print("🔄 Control incremental reseteado - próxima extracción será completa")