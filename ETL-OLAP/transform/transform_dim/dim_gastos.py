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
    return ['gastos', 'penalizaciones']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combina:
    - Tabla gastos: TipoGasto, Categoria, Monto
    - Tabla penalizaciones: se agrega como TipoGasto='Penalizaciones' con su monto
    """
    gastos = ensure_df(df_dict.get('gastos', pd.DataFrame()))
    penalizaciones = ensure_df(df_dict.get('penalizaciones', pd.DataFrame()))
    
    gastos_data = []
    
    # Procesar gastos regulares
    if not gastos.empty:
        for _, row in gastos.iterrows():
            gastos_data.append({
                'TipoGasto': row.get('TipoGasto', 'No especificado'),
                'Categoria': row.get('Categoria', 'No especificado'),
                'Monto': float(row.get('Monto', 0))
            })
    
    # Procesar penalizaciones como tipo de gasto especial
    if not penalizaciones.empty:
        for _, row in penalizaciones.iterrows():
            gastos_data.append({
                'TipoGasto': 'Penalizaciones',
                'Categoria': 'OPEX',  # Las penalizaciones son gastos operativos
                'Monto': float(row.get('Monto', 0))
            })
    
    # Crear DataFrame resultado
    if not gastos_data:
        logger.warning('dim_gastos: No hay datos de gastos para procesar')
        return pd.DataFrame(columns=['ID_Finanza', 'TipoGasto', 'Categoria', 'Monto'])
    
    result = pd.DataFrame(gastos_data)
    
    # Agregar ID secuencial
    result.insert(0, 'ID_Finanza', range(1, len(result) + 1))
    
    # Limpiar datos
    result['TipoGasto'] = result['TipoGasto'].astype(str).str.strip()
    result['Categoria'] = result['Categoria'].astype(str).str.strip()
    result['Monto'] = pd.to_numeric(result['Monto'], errors='coerce').fillna(0)
    
    # Log del resultado
    total_input = len(gastos) + len(penalizaciones)
    log_transform_info('dim_gastos', total_input, len(result))
    
    return result