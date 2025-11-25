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
    return ['asignaciones', 'empleados', 'proyectos', 'dim_proyectos']

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Obtener datos de entrada
    asignaciones = ensure_df(df_dict.get('asignaciones', pd.DataFrame()))
    empleados = ensure_df(df_dict.get('empleados', pd.DataFrame()))
    dim_tiempo = ensure_df(df_dict.get('dim_tiempo', pd.DataFrame()))
    dim_proyectos = ensure_df(df_dict.get('dim_proyectos', pd.DataFrame()))
    
    if asignaciones.empty:
        logger.warning('hechos_asignaciones: No hay datos de asignaciones')
        return pd.DataFrame(columns=[
            'ID_HechoAsignacion', 'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion', 
            'HorasPlanificadas', 'HorasReales', 'ValorHoras', 'RetrasoHoras'
        ])
    
    # Trabajar con copia
    df = asignaciones.copy()
    
    # ID_HechoAsignacion: ID generado secuencial
    df['ID_HechoAsignacion'] = range(1, len(df) + 1)
    
    # Filtrar asignaciones que pertenecen a proyectos válidos en dim_proyectos
    if not dim_proyectos.empty:
        proyectos_validos = set(dim_proyectos['ID_Proyecto'].unique())
        df_antes_filtro = len(df)
        df = df[df['ID_Proyecto'].isin(proyectos_validos)]
        if len(df) < df_antes_filtro:
            logger.info(f'hechos_asignaciones: Filtradas {df_antes_filtro - len(df)} asignaciones con proyectos inexistentes en dim_proyectos')
        
        # Regenerar ID_HechoAsignacion después del filtro
        df['ID_HechoAsignacion'] = range(1, len(df) + 1)
    
    # ID_FechaAsignacion: Mapear fecha a ID de dim_tiempo
    if not dim_tiempo.empty:
        # Convertir fechas a formato date para mapeo
        df['FechaAsignacion_date'] = pd.to_datetime(df['FechaAsignacion'], errors='coerce').dt.date
        
        # Como dim_tiempo solo tiene Dia, Mes, Anio, creamos un mapeo simple
        # usando el ID_Tiempo secuencial basado en la fecha desde 2019-01-01
        def fecha_to_tiempo_id(fecha_date):
            if pd.isna(fecha_date):
                return 0
            try:
                base_date = pd.to_datetime('2019-01-01').date()
                days_diff = (fecha_date - base_date).days + 1
                return max(1, days_diff) if days_diff > 0 else 1
            except:
                return 1
        
        df['ID_FechaAsignacion'] = df['FechaAsignacion_date'].apply(fecha_to_tiempo_id)
        
        # Manejar fechas fuera de rango
        max_tiempo_id = len(dim_tiempo) if len(dim_tiempo) > 0 else 2557  # Rango hasta 2025
        df['ID_FechaAsignacion'] = df['ID_FechaAsignacion'].clip(upper=max_tiempo_id)
        
    else:
        logger.warning('hechos_asignaciones: dim_tiempo vacía, usando 0 como ID_FechaAsignacion')
        df['ID_FechaAsignacion'] = 0
    
    # HorasPlanificadas y HorasReales: Extraídas directamente del SGP
    df['HorasPlanificadas'] = pd.to_numeric(df['HorasPlanificadas'], errors='coerce').fillna(0)
    df['HorasReales'] = pd.to_numeric(df['HorasReales'], errors='coerce').fillna(0)
    
    # ValorHoras: CostoPorHora × HorasReales
    if not empleados.empty and 'CostoPorHora' in empleados.columns:
        # Crear mapeo empleado -> costo por hora
        emp_cost_map = empleados.set_index('ID_Empleado')['CostoPorHora'].to_dict()
        
        # Mapear costos y calcular valor
        df['CostoPorHora'] = df['ID_Empleado'].map(emp_cost_map).fillna(0)
        df['ValorHoras'] = df['HorasReales'] * df['CostoPorHora']
    else:
        df['ValorHoras'] = 0
        logger.warning('hechos_asignaciones: No se pudo calcular ValorHoras')
    
    # Calcular RetrasoHoras (HorasReales - HorasPlanificadas)
    df['RetrasoHoras'] = df['HorasReales'] - df['HorasPlanificadas']
    
    # Seleccionar columnas finales según nuevo schema
    result = df[[
        'ID_HechoAsignacion', 'ID_Empleado', 'ID_Proyecto', 'ID_FechaAsignacion',
        'HorasPlanificadas', 'HorasReales', 'ValorHoras', 'RetrasoHoras'
    ]].copy()
    
    # Log del resultado
    log_transform_info('hechos_asignaciones', len(asignaciones), len(result))
    
    return result