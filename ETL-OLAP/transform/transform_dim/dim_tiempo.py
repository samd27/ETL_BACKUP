import pandas as pd
import logging
from typing import Dict
from datetime import datetime, timedelta
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['proyectos', 'hitos', 'asignaciones', 'gastos', 'penalizaciones']

# Removed subdimensions - now using simplified structure

def extract_dates_from_data(df_dict: Dict[str, pd.DataFrame]) -> list:
    fechas = set()
    
    # Proyectos
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    if not proyectos.empty:
        for col in ['FechaInicio', 'FechaFin']:
            if col in proyectos.columns:
                fechas.update(proyectos[col].dropna().tolist())
    
    # Hitos
    hitos = df_dict.get('hitos', pd.DataFrame())
    if not hitos.empty:
        for col in ['FechaInicio', 'FechaFinPlanificada', 'FechaFinReal']:
            if col in hitos.columns:
                fechas.update(hitos[col].dropna().tolist())
    
    # Asignaciones
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    if not asignaciones.empty and 'FechaAsignacion' in asignaciones.columns:
        fechas.update(asignaciones['FechaAsignacion'].dropna().tolist())
    
    # Gastos
    gastos = df_dict.get('gastos', pd.DataFrame())
    if not gastos.empty and 'Fecha' in gastos.columns:
        fechas.update(gastos['Fecha'].dropna().tolist())
    
    # Penalizaciones  
    penalizaciones = df_dict.get('penalizaciones', pd.DataFrame())
    if not penalizaciones.empty and 'Fecha' in penalizaciones.columns:
        fechas.update(penalizaciones['Fecha'].dropna().tolist())
    
    # Filtrar fechas válidas y convertir
    fechas_validas = []
    for fecha in fechas:
        try:
            if isinstance(fecha, str):
                fecha_dt = pd.to_datetime(fecha)
            else:
                fecha_dt = pd.to_datetime(fecha)
            fechas_validas.append(fecha_dt.date())
        except:
            continue
    
    return sorted(list(set(fechas_validas)))

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    
    # Extraer fechas de los datos
    fechas_encontradas = extract_dates_from_data(df_dict)
    
    if not fechas_encontradas:
        logger.warning('dim_tiempo: No se encontraron fechas en los datos')
        # Crear rango desde 2019 hasta actualidad para cubrir todo el SGP
        start_date = datetime(2019, 1, 1).date()
        end_date = datetime(2025, 12, 31).date()
        fechas_rango = []
        current_date = start_date
        while current_date <= end_date:
            fechas_rango.append(current_date)
            current_date += timedelta(days=1)
        fechas_encontradas = fechas_rango
    else:
        # Si encontramos fechas, expandir el rango para asegurar cobertura completa
        min_fecha = min(fechas_encontradas)
        max_fecha = max(fechas_encontradas)
        
        # Expandir hacia atrás hasta 2019 si es necesario
        start_expand = datetime(2019, 1, 1).date()
        if min_fecha > start_expand:
            min_fecha = start_expand
            
        # Expandir hacia adelante hasta 2025 si es necesario  
        end_expand = datetime(2025, 12, 31).date()
        if max_fecha < end_expand:
            max_fecha = end_expand
        
        # Generar rango completo de fechas
        fechas_completas = []
        current_date = min_fecha
        while current_date <= max_fecha:
            fechas_completas.append(current_date)
            current_date += timedelta(days=1)
        fechas_encontradas = sorted(fechas_completas)
    
    logger.info(f'dim_tiempo: Generando {len(fechas_encontradas)} fechas desde {fechas_encontradas[0]} hasta {fechas_encontradas[-1]}')
    
    # Crear dimensión tiempo simplificada
    dim_tiempo = []
    for i, fecha in enumerate(fechas_encontradas, 1):
        dim_tiempo.append({
            'ID_Tiempo': i,
            'Dia': fecha.day,
            'Mes': fecha.month,
            'Anio': fecha.year
        })
    
    result = pd.DataFrame(dim_tiempo)
    log_transform_info('dim_tiempo', len(fechas_encontradas), len(result))
    return result