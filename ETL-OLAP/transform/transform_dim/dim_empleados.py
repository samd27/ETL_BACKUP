import pandas as pd
import logging
from typing import Dict
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['empleados', 'asignaciones']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    empleados = ensure_df(df_dict.get('empleados', pd.DataFrame()))
    
    if empleados.empty:
        logger.warning('dim_empleados: No hay datos de empleados')
        return pd.DataFrame(columns=['ID_Empleado','CodigoEmpleado','Rol','Seniority'])

    df = empleados.copy()
    df['CodigoEmpleado'] = df['ID_Empleado']
    
    # Limpiar datos (sin incluir NombreCompleto)
    df['Rol'] = df['Rol'].astype(str).str.strip()
    df['Seniority'] = df['Seniority'].fillna('No especificado').astype(str).str.strip()

    # Seleccionar SOLO las columnas requeridas para el DW (sin NombreCompleto)
    result = df[['ID_Empleado','CodigoEmpleado','Rol','Seniority']]
    log_transform_info('dim_empleados', len(empleados), len(result))
    return result