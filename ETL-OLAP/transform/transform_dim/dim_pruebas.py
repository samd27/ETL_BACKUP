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
    return ['pruebas', 'hitos', 'dim_hitos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    pruebas = ensure_df(df_dict.get('pruebas', pd.DataFrame()))
    dim_hitos = ensure_df(df_dict.get('dim_hitos', pd.DataFrame()))
    
    if pruebas.empty:
        logger.warning('dim_pruebas: No hay datos de pruebas')
        return pd.DataFrame(columns=['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa'])

    df = pruebas.copy()
    df['CodigoPrueba'] = df['ID_Prueba']
    
    # Filtrar pruebas que pertenecen a hitos válidos en dim_hitos
    if not dim_hitos.empty:
        hitos_validos = set(dim_hitos['ID_Hito'].unique())
        df_antes_filtro = len(df)
        df = df[df['ID_Hito'].isin(hitos_validos)]
        if len(df) < df_antes_filtro:
            logger.info(f'dim_pruebas: Filtradas {df_antes_filtro - len(df)} pruebas con hitos inexistentes en dim_hitos')
    
    # Normalizar resultado de prueba
    df['PruebaExitosa'] = df['Exitosa'].fillna(0).astype(int)
    
    # Limpiar tipo de prueba
    df['TipoPrueba'] = df['TipoPrueba'].astype(str).str.strip()

    # La validación FK ya se hizo arriba con dim_hitos

    result = df[['ID_Prueba','CodigoPrueba','ID_Hito','TipoPrueba','PruebaExitosa']]
    
    log_transform_info('dim_pruebas', len(pruebas), len(result))
    return result
