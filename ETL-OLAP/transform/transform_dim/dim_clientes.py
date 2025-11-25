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
    """Tablas origen necesarias"""
    return ['clientes', 'contratos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Obtener datos de entrada
    clientes = ensure_df(df_dict.get('clientes', pd.DataFrame()))
    
    if clientes.empty:
        logger.warning('dim_clientes: No hay datos de clientes para procesar')
        return pd.DataFrame(columns=['ID_Cliente', 'CodigoClienteReal'])
    
    # Transformación simple
    df = clientes.copy()
    
    # Crear CodigoClienteReal (mapea al ID original del SGP)
    df['CodigoClienteReal'] = df['ID_Cliente']
    
    # Seleccionar SOLO las columnas requeridas para el DW
    result = df[['ID_Cliente', 'CodigoClienteReal']].copy()
    
    # Log del resultado
    log_transform_info('dim_clientes', len(clientes), len(result))
    
    return result
