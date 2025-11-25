import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any

def cubo_base_proyectos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo base: Cliente x Estado x Periodo, medidas = Presupuesto, Costo Real, Productividad
    """
    return pd.pivot_table(
        df,
        values=['Presupuesto', 'CosteReal', 'ProductividadPromedio'],
        index=['CodigoClienteReal', 'Estado'],
        columns=['PeriodoInicio'],
        aggfunc={
            'Presupuesto': 'sum',
            'CosteReal': 'sum', 
            'ProductividadPromedio': 'mean'
        },
        margins=True,
        margins_name='TOTAL'
    )

def cubo_productividad_calidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo: Categoria Productividad x Categoria Calidad x Estado
    Medidas: Cantidad proyectos, Presupuesto promedio, Desviación promedio
    """
    return pd.pivot_table(
        df,
        values=['ID_Proyecto', 'Presupuesto', 'DesviacionPresupuestal'],
        index=['CategoriaProductividad', 'CategoriaCalidad'],
        columns=['Estado'],
        aggfunc={
            'ID_Proyecto': 'count',
            'Presupuesto': 'mean',
            'DesviacionPresupuestal': 'mean'
        },
        margins=True,
        fill_value=0
    )

def cubo_financiero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo financiero: Categoria Presupuesto x Tipo Desviación x Cliente
    Medidas: Cantidad, Presupuesto total, Costo real total, Desviación total
    """
    return pd.pivot_table(
        df,
        values=['ID_Proyecto', 'Presupuesto', 'CosteReal', 'DesviacionPresupuestal'],
        index=['CategoriaPresupuesto', 'TipoDesviacion'],
        columns=['CodigoClienteReal'],
        aggfunc={
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum'
        },
        margins=True,
        fill_value=0
    )

def cubo_temporal_estado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo temporal: Año Inicio x Estado x Categoria Calidad
    Medidas: Cantidad proyectos, Tasa éxito promedio, Productividad promedio
    """
    return pd.pivot_table(
        df,
        values=['ID_Proyecto', 'TasaDeExitoEnPruebas', 'ProductividadPromedio'],
        index=['AnioInicio', 'Estado'],
        columns=['CategoriaCalidad'],
        aggfunc={
            'ID_Proyecto': 'count',
            'TasaDeExitoEnPruebas': 'mean',
            'ProductividadPromedio': 'mean'
        },
        margins=True,
        fill_value=0
    )

def cubo_multimedidas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo con múltiples medidas: Cliente x Estado
    Todas las métricas principales agrupadas
    """
    return pd.pivot_table(
        df,
        values=[
            'Presupuesto', 'CosteReal', 'DesviacionPresupuestal',
            'ProductividadPromedio', 'TasaDeExitoEnPruebas', 
            'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ],
        index=['CodigoClienteReal'],
        columns=['Estado'],
        aggfunc={
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean',
            'PorcentajeTareasRetrasadas': 'mean',
            'PorcentajeHitosRetrasados': 'mean'
        },
        margins=True
    )

def cubo_kpis_ejecutivos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo KPIs ejecutivos: Resumen de alto nivel por cliente y periodo
    """
    # Calcular KPIs derivados
    df_kpis = df.copy()
    df_kpis['ROI'] = (df_kpis['Presupuesto'] - df_kpis['CosteReal']) / df_kpis['Presupuesto'] * 100
    df_kpis['EficienciaPresupuestal'] = df_kpis['Presupuesto'] / df_kpis['CosteReal'] 
    df_kpis['IndicadorCalidad'] = (df_kpis['TasaDeExitoEnPruebas'] * 0.4 + 
                                   (100 - df_kpis['PorcentajeTareasRetrasadas']) / 100 * 0.6)
    
    return pd.pivot_table(
        df_kpis,
        values=[
            'ID_Proyecto', 'Presupuesto', 'CosteReal', 'ROI', 
            'EficienciaPresupuestal', 'IndicadorCalidad', 'ProductividadPromedio'
        ],
        index=['CodigoClienteReal'],
        columns=['PeriodoInicio'],
        aggfunc={
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'ROI': 'mean',
            'EficienciaPresupuestal': 'mean',
            'IndicadorCalidad': 'mean',
            'ProductividadPromedio': 'mean'
        },
        margins=True
    )

def crear_cubo_personalizado(df: pd.DataFrame, 
                           dimensiones: List[str], 
                           medidas: List[str],
                           columnas: Optional[List[str]] = None,
                           agregaciones: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Crea un cubo personalizado con las dimensiones y medidas especificadas.
    
    Args:
        df: DataFrame con los datos
        dimensiones: Lista de columnas para el índice
        medidas: Lista de columnas de medidas
        columnas: Lista de columnas para las columnas del pivot (opcional)
        agregaciones: Diccionario de funciones de agregación por medida
    """
    if agregaciones is None:
        # Agregaciones por defecto
        agregaciones = {}
        for medida in medidas:
            if 'Presupuesto' in medida or 'Costo' in medida or 'Monto' in medida:
                agregaciones[medida] = 'sum'
            elif 'Porcentaje' in medida or 'Tasa' in medida or 'Productividad' in medida:
                agregaciones[medida] = 'mean'
            elif 'ID_' in medida:
                agregaciones[medida] = 'count'
            else:
                agregaciones[medida] = 'sum'
    
    return pd.pivot_table(
        df,
        values=medidas,
        index=dimensiones,
        columns=columnas,
        aggfunc=agregaciones,
        margins=True,
        fill_value=0
    )

def cubo_kpis_principales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo principal para los 11 KPIs del negocio
    Dimensiones: Cliente x Año x Estado
    Medidas: Todos los KPIs principales
    """
    try:
        # Calcular KPIs individuales por proyecto
        df_kpis = df.copy()
        
        # 1. Cumplimiento de presupuesto
        df_kpis['cumplimiento_presupuesto'] = np.where(
            df_kpis['Presupuesto'] > 0,
            np.minimum(100, df_kpis['Presupuesto'] / df_kpis['CosteReal'] * 100),
            100
        )
        
        # 2. Desviación presupuestal
        df_kpis['desviacion_presupuestal'] = np.where(
            df_kpis['Presupuesto'] > 0,
            abs(df_kpis['CosteReal'] - df_kpis['Presupuesto']) / df_kpis['Presupuesto'] * 100,
            0
        )
        
        # 3. Penalizaciones
        df_kpis['penalizaciones_pct'] = np.where(
            df_kpis['Presupuesto'] > 0,
            df_kpis['PenalizacionesMonto'] / df_kpis['Presupuesto'] * 100,
            0
        )
        
        # 4. Proyecto a tiempo (1 si está a tiempo, 0 si no)
        df_kpis['proyecto_a_tiempo'] = (df_kpis['RetrasoFinalDias'] <= 0).astype(int) * 100
        
        # 5. Proyecto cancelado
        df_kpis['proyecto_cancelado'] = df_kpis['Cancelado'] * 100
        
        return pd.pivot_table(
            df_kpis,
            values=[
                'cumplimiento_presupuesto', 'desviacion_presupuestal', 'penalizaciones_pct',
                'proyecto_a_tiempo', 'proyecto_cancelado', 'PorcentajeTareasRetrasadas',
                'PorcentajeHitosRetrasados', 'TasaDeErroresEncontrados', 'ProductividadPromedio',
                'TasaDeExitoEnPruebas'
            ],
            index=['CodigoClienteReal', 'AnioInicio'],
            columns=['Estado'],
            aggfunc={
                'cumplimiento_presupuesto': 'mean',
                'desviacion_presupuestal': 'mean',
                'penalizaciones_pct': 'mean',
                'proyecto_a_tiempo': 'mean',
                'proyecto_cancelado': 'mean',
                'PorcentajeTareasRetrasadas': 'mean',
                'PorcentajeHitosRetrasados': 'mean',
                'TasaDeErroresEncontrados': 'mean',
                'ProductividadPromedio': 'mean',
                'TasaDeExitoEnPruebas': lambda x: x.mean() * 100
            },
            margins=True,
            margins_name='PROMEDIO'
        )
    except Exception as e:
        print(f"Error en cubo_kpis_principales: {e}")
        return pd.DataFrame()

def cubo_kpis_temporal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo para análisis temporal de KPIs
    Dimensiones: Año x Mes
    Medidas: KPIs agregados mensualmente
    """
    try:
        df_temporal = df.copy()
        
        # Calcular métricas
        df_temporal['cumplimiento_presupuesto'] = np.where(
            df_temporal['Presupuesto'] > 0,
            np.minimum(100, df_temporal['Presupuesto'] / df_temporal['CosteReal'] * 100),
            100
        )
        df_temporal['proyecto_a_tiempo'] = (df_temporal['RetrasoFinalDias'] <= 0).astype(int) * 100
        df_temporal['proyecto_cancelado'] = df_temporal['Cancelado'] * 100
        
        return pd.pivot_table(
            df_temporal,
            values=[
                'cumplimiento_presupuesto', 'proyecto_a_tiempo', 'proyecto_cancelado',
                'TasaDeExitoEnPruebas', 'ProductividadPromedio', 'ID_Proyecto'
            ],
            index=['AnioInicio'],
            columns=['MesInicio'],
            aggfunc={
                'cumplimiento_presupuesto': 'mean',
                'proyecto_a_tiempo': 'mean',
                'proyecto_cancelado': 'mean',
                'TasaDeExitoEnPruebas': lambda x: x.mean() * 100,
                'ProductividadPromedio': 'mean',
                'ID_Proyecto': 'count'
            },
            margins=True,
            margins_name='PROMEDIO'
        )
    except Exception as e:
        print(f"Error en cubo_kpis_temporal: {e}")
        return pd.DataFrame()

def cubo_kpis_por_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cubo para análisis de KPIs por categorías de presupuesto y productividad
    """
    try:
        df_cat = df.copy()
        
        # Calcular KPIs
        df_cat['cumplimiento_presupuesto'] = np.where(
            df_cat['Presupuesto'] > 0,
            np.minimum(100, df_cat['Presupuesto'] / df_cat['CosteReal'] * 100),
            100
        )
        df_cat['proyecto_exitoso'] = ((df_cat['RetrasoFinalDias'] <= 0) & 
                                      (df_cat['Cancelado'] == 0) & 
                                      (df_cat['TasaDeExitoEnPruebas'] >= 0.9)).astype(int) * 100
        
        return pd.pivot_table(
            df_cat,
            values=[
                'cumplimiento_presupuesto', 'proyecto_exitoso', 'TasaDeExitoEnPruebas',
                'ProductividadPromedio', 'PorcentajeTareasRetrasadas', 'ID_Proyecto'
            ],
            index=['CategoriaPresupuesto'],
            columns=['CategoriaProductividad'],
            aggfunc={
                'cumplimiento_presupuesto': 'mean',
                'proyecto_exitoso': 'mean',
                'TasaDeExitoEnPruebas': lambda x: x.mean() * 100,
                'ProductividadPromedio': 'mean',
                'PorcentajeTareasRetrasadas': 'mean',
                'ID_Proyecto': 'count'
            },
            margins=True,
            margins_name='TOTAL'
        )
    except Exception as e:
        print(f"Error en cubo_kpis_por_categoria: {e}")
        return pd.DataFrame()