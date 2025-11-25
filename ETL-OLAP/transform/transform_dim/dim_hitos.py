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
    return ['hitos', 'proyectos', 'dim_proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    hitos = ensure_df(df_dict.get('hitos', pd.DataFrame()))
    dim_proyectos = ensure_df(df_dict.get('dim_proyectos', pd.DataFrame()))
    
    if hitos.empty:
        logger.warning('dim_hitos: No hay datos de hitos')
        return pd.DataFrame(columns=['ID_Hito','CodigoHito','ID_proyectos','ID_FechaInicio','ID_FechaFin','RetrasoInicioDias','RetrasoFinDias'])

    df = hitos.copy()
    df['CodigoHito'] = df['ID_Hito']
    
    # Filtrar hitos que pertenecen a proyectos válidos en dim_proyectos
    if not dim_proyectos.empty:
        proyectos_validos = set(dim_proyectos['ID_Proyecto'].unique())
        df_antes_filtro = len(df)
        df = df[df['ID_Proyecto'].isin(proyectos_validos)]
        if len(df) < df_antes_filtro:
            logger.info(f'dim_hitos: Filtrados {df_antes_filtro - len(df)} hitos con proyectos inexistentes en dim_proyectos')
    
    if df.empty:
        logger.warning('dim_hitos: No quedan hitos después de filtrar por proyectos válidos')
        return pd.DataFrame(columns=['ID_Hito','CodigoHito','ID_proyectos','ID_FechaInicio','ID_FechaFin','RetrasoInicioDias','RetrasoFinDias'])
    
    # Función para convertir fecha a ID de dim_tiempo
    def fecha_to_tiempo_id(fecha_str):
        if pd.isna(fecha_str) or str(fecha_str) == 'None':
            return None
        try:
            fecha = pd.to_datetime(fecha_str)
            # Generar ID basado en días desde 2019-01-01 (fecha base de dim_tiempo)
            base_date = pd.to_datetime('2019-01-01')
            days_diff = (fecha - base_date).days + 1
            return max(1, days_diff) if days_diff > 0 else 1
        except:
            return 1  # Default al primer ID de tiempo
    
    # Mapear fechas a IDs de dim_tiempo
    df['ID_FechaInicio'] = df['FechaInicio'].apply(fecha_to_tiempo_id)
    df['ID_FechaFinalizacion'] = df['FechaFinReal'].apply(fecha_to_tiempo_id)
    
    # Calcular RetrasoInicioDias: FechaInicioReal - FechaInicio
    def calcular_retraso_inicio(fecha_real, fecha_planificada):
        """Calcula días de retraso de inicio"""
        try:
            if pd.isna(fecha_real) or pd.isna(fecha_planificada):
                return 0
            real = pd.to_datetime(fecha_real)
            planificada = pd.to_datetime(fecha_planificada)
            return max(0, (real - planificada).days)
        except:
            return 0
    
    # Calcular RetrasoFinDias: FechaFinReal - FechaFinPlanificada
    def calcular_retraso_fin(fecha_real, fecha_planificada):
        """Calcula días de retraso de finalización"""
        try:
            if pd.isna(fecha_real) or pd.isna(fecha_planificada):
                return 0
            real = pd.to_datetime(fecha_real)
            planificada = pd.to_datetime(fecha_planificada)
            return max(0, (real - planificada).days)
        except:
            return 0
    
    df['RetrasoInicioDias'] = df.apply(
        lambda row: calcular_retraso_inicio(row.get('FechaInicioReal'), row.get('FechaInicio')), 
        axis=1
    )
    
    df['RetrasoFinDias'] = df.apply(
        lambda row: calcular_retraso_fin(row.get('FechaFinReal'), row.get('FechaFinPlanificada')), 
        axis=1
    )

    # Seleccionar columnas finales para el DW
    result = df[['ID_Hito','CodigoHito','ID_Proyecto','ID_FechaInicio','ID_FechaFinalizacion','RetrasoInicioDias','RetrasoFinDias']].copy()
    
    # Renombrar las columnas para que coincidan con el esquema del DW
    result = result.rename(columns={
        'ID_Proyecto': 'ID_proyectos',
        'ID_FechaFinalizacion': 'ID_FechaFin'
    })
    
    log_transform_info('dim_hitos', len(hitos), len(result))
    return result
