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
    return ['proyectos', 'contratos', 'clientes', 'errores', 'asignaciones']  # Agregar errores y asignaciones para métricas

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))
    clientes = ensure_df(df_dict.get('clientes', pd.DataFrame()))
    errores = ensure_df(df_dict.get('errores', pd.DataFrame()))
    asignaciones = ensure_df(df_dict.get('asignaciones', pd.DataFrame()))
    
    if proyectos.empty:
        logger.warning('dim_proyectos: No hay datos de proyectos')
        return pd.DataFrame(columns=['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente','TotalErrores','NumTrabajadores'])

    df = proyectos.copy()
    df['CodigoProyecto'] = df['ID_Proyecto']
    
    # Determinar si está cancelado basado en EstadoProyecto (filtro ya aplicado en extracción)
    if 'EstadoProyecto' in df.columns:
        df['Cancelado'] = df['EstadoProyecto'].apply(lambda x: 1 if str(x) == 'Cancelado' else 0)
        logger.info(f'dim_proyectos: Procesando {len(df)} proyectos (ya filtrados por estado en extracción)')
    elif 'Estado' in df.columns:
        df['Cancelado'] = df['Estado'].apply(lambda x: 1 if str(x) == 'Cancelado' else 0)
        logger.info(f'dim_proyectos: Procesando {len(df)} proyectos (ya filtrados por estado en extracción)')
    else:
        logger.warning('dim_proyectos: Campo Estado/EstadoProyecto no encontrado, usando valor por defecto')
        df['Cancelado'] = 0
    
    # Limpiar versión
    df['Version'] = df['Version'].fillna('1.0').astype(str).str.strip()
    
    # Obtener ID_Cliente desde contratos
    contratos = df_dict.get('contratos', pd.DataFrame())
    if not contratos.empty:
        contrato_cliente_map = contratos.set_index('ID_Contrato')['ID_Cliente'].to_dict()
        df['ID_Cliente'] = df['ID_Contrato'].map(contrato_cliente_map)
    else:
        df['ID_Cliente'] = None
    
    # Calcular TotalErrores por proyecto
    if not errores.empty:
        errores_por_proyecto = errores.groupby('ID_Proyecto')['ID_Error'].count().to_dict()
        df['TotalErrores'] = df['ID_Proyecto'].map(errores_por_proyecto).fillna(0).astype(int)
    else:
        df['TotalErrores'] = 0
    
    # Calcular NumTrabajadores por proyecto (empleados únicos asignados)
    if not asignaciones.empty:
        trabajadores_por_proyecto = asignaciones.groupby('ID_Proyecto')['ID_Empleado'].nunique().to_dict()
        df['NumTrabajadores'] = df['ID_Proyecto'].map(trabajadores_por_proyecto).fillna(0).astype(int)
    else:
        df['NumTrabajadores'] = 0

    # Validar que ID_Cliente existe en dim_clientes (opcional para proyecto escolar)
    if not clientes.empty:
        clientes_validos = set(clientes['ID_Cliente'].unique())
        df = df[df['ID_Cliente'].isin(clientes_validos)]
        if len(df) < len(proyectos):
            logger.info(f'dim_proyectos: Filtrados {len(proyectos) - len(df)} proyectos con clientes inexistentes')

    # Eliminar duplicados por ID_Proyecto (mantener el primer registro)
    df_antes_duplicados = len(df)
    df = df.drop_duplicates(subset=['ID_Proyecto'], keep='first')
    if len(df) < df_antes_duplicados:
        logger.info(f'dim_proyectos: Eliminados {df_antes_duplicados - len(df)} proyectos duplicados')

    result = df[['ID_Proyecto','CodigoProyecto','Version','Cancelado','ID_Cliente','TotalErrores','NumTrabajadores']].copy()
    log_transform_info('dim_proyectos', len(proyectos), len(result))
    return result
