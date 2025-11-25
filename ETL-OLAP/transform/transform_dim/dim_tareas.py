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
    return ['tareas', 'hitos', 'dim_hitos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    tareas = ensure_df(df_dict.get('tareas', pd.DataFrame()))
    dim_hitos = ensure_df(df_dict.get('dim_hitos', pd.DataFrame()))
    
    if tareas.empty:
        logger.warning('dim_tareas: No hay datos de tareas')
        return pd.DataFrame(columns=['ID_Tarea','CodigoTarea','ID_Hito','SeRetraso'])

    df = tareas.copy()
    df['CodigoTarea'] = df['ID_Tarea']
    
    # Filtrar tareas que pertenecen a hitos válidos en dim_hitos
    if not dim_hitos.empty:
        hitos_validos = set(dim_hitos['ID_Hito'].unique())
        df_antes_filtro = len(df)
        df = df[df['ID_Hito'].isin(hitos_validos)]
        if len(df) < df_antes_filtro:
            logger.info(f'dim_tareas: Filtradas {df_antes_filtro - len(df)} tareas con hitos inexistentes en dim_hitos')
    
    # Convertir fechas a datetime
    df['FechaInicioPlanificada'] = pd.to_datetime(df['FechaInicioPlanificada'], errors='coerce')
    df['FechaInicioReal'] = pd.to_datetime(df['FechaInicioReal'], errors='coerce')
    df['FechaFinPlanificada'] = pd.to_datetime(df['FechaFinPlanificada'], errors='coerce')
    df['FechaFinReal'] = pd.to_datetime(df['FechaFinReal'], errors='coerce')
    
    # SeRetraso = 1 si hay diferencia entre fechas planificadas y reales
    # True si FechaInicioPlanificada != FechaInicioReal OR FechaFinPlanificada != FechaFinReal
    retraso_inicio = (df['FechaInicioPlanificada'] != df['FechaInicioReal']) & df['FechaInicioReal'].notna()
    retraso_fin = (df['FechaFinPlanificada'] != df['FechaFinReal']) & df['FechaFinReal'].notna()
    
    df['SeRetraso'] = (retraso_inicio | retraso_fin).astype(int)

    result = df[['ID_Tarea','CodigoTarea','ID_Hito','SeRetraso']]
    
    log_transform_info('dim_tareas', len(tareas), len(result))
    return result