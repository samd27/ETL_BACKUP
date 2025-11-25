import pandas as pd
from typing import List, Optional, Any, Dict

def slice_por_cliente(df: pd.DataFrame, cliente: int) -> pd.DataFrame:
    """Slice: Filtrar datos por un cliente específico"""
    return df.loc[df["CodigoClienteReal"] == cliente].copy()

def slice_por_estado(df: pd.DataFrame, estado: str) -> pd.DataFrame:
    """Slice: Filtrar datos por estado del proyecto (Cerrado/Cancelado)"""
    return df.loc[df["Estado"] == estado].copy()

def slice_por_anio(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    """Slice: Filtrar datos por año de inicio"""
    return df.loc[df["AnioInicio"] == anio].copy()

def slice_por_categoria_presupuesto(df: pd.DataFrame, categoria: str) -> pd.DataFrame:
    """Slice: Filtrar por categoría de presupuesto"""
    return df.loc[df["CategoriaPresupuesto"] == categoria].copy()

def dice_subset(df: pd.DataFrame,
                clientes: Optional[List[int]] = None,
                estados: Optional[List[str]] = None,
                anios: Optional[List[int]] = None,
                categorias_presupuesto: Optional[List[str]] = None,
                categorias_productividad: Optional[List[str]] = None,
                categorias_calidad: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Dice: Filtrar por múltiples dimensiones simultáneamente
    """
    mask = pd.Series([True] * len(df), index=df.index)
    
    if clientes is not None:
        mask &= df["CodigoClienteReal"].isin(clientes)
    if estados is not None:
        mask &= df["Estado"].isin(estados)
    if anios is not None:
        mask &= df["AnioInicio"].isin(anios)
    if categorias_presupuesto is not None:
        mask &= df["CategoriaPresupuesto"].isin(categorias_presupuesto)
    if categorias_productividad is not None:
        mask &= df["CategoriaProductividad"].isin(categorias_productividad)
    if categorias_calidad is not None:
        mask &= df["CategoriaCalidad"].isin(categorias_calidad)
    
    return df.loc[mask].copy()

def rollup_por_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """Roll-up: Agregar todos los proyectos por cliente"""
    return df.groupby("CodigoClienteReal").agg({
        'ID_Proyecto': 'count',
        'Presupuesto': 'sum',
        'CosteReal': 'sum',
        'DesviacionPresupuestal': 'sum',
        'ProductividadPromedio': 'mean',
        'TasaDeExitoEnPruebas': 'mean',
        'PorcentajeTareasRetrasadas': 'mean',
        'PorcentajeHitosRetrasados': 'mean'
    }).round(2)

def rollup_por_estado(df: pd.DataFrame) -> pd.DataFrame:
    """Roll-up: Agregar todos los proyectos por estado"""
    return df.groupby("Estado").agg({
        'ID_Proyecto': 'count',
        'Presupuesto': 'sum',
        'CosteReal': 'sum',
        'DesviacionPresupuestal': 'sum',
        'ProductividadPromedio': 'mean',
        'TasaDeExitoEnPruebas': 'mean',
        'PorcentajeTareasRetrasadas': 'mean',
        'PorcentajeHitosRetrasados': 'mean'
    }).round(2)

def rollup_por_anio(df: pd.DataFrame) -> pd.DataFrame:
    """Roll-up: Agregar todos los proyectos por año"""
    return df.groupby("AnioInicio").agg({
        'ID_Proyecto': 'count',
        'Presupuesto': 'sum',
        'CosteReal': 'sum',
        'DesviacionPresupuestal': 'sum',
        'ProductividadPromedio': 'mean',
        'TasaDeExitoEnPruebas': 'mean',
        'PorcentajeTareasRetrasadas': 'mean',
        'PorcentajeHitosRetrasados': 'mean'
    }).round(2)

def rollup_jerarquico(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Roll-up jerárquico: Cliente -> Estado -> Categoría Presupuesto
    Retorna múltiples niveles de agregación
    """
    return {
        'nivel_1_total': df.agg({
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        }).to_frame('Total').T,
        
        'nivel_2_cliente': rollup_por_cliente(df),
        
        'nivel_3_cliente_estado': df.groupby(['CodigoClienteReal', 'Estado']).agg({
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        }).round(2),
        
        'nivel_4_detalle': df.groupby(['CodigoClienteReal', 'Estado', 'CategoriaPresupuesto']).agg({
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        }).round(2)
    }

def drilldown_cliente_detallado(df: pd.DataFrame, cliente: int) -> pd.DataFrame:
    """
    Drill-down: De cliente -> estado -> categoría presupuesto -> proyecto individual
    """
    cliente_data = df[df['CodigoClienteReal'] == cliente]
    
    if cliente_data.empty:
        return pd.DataFrame()
    
    # Drill-down por proyecto individual con todas las métricas
    return (cliente_data.groupby(['CodigoProyecto', 'Estado', 'CategoriaPresupuesto', 'Version'])
            .agg({
                'Presupuesto': 'first',
                'CosteReal': 'first', 
                'DesviacionPresupuestal': 'first',
                'ProductividadPromedio': 'first',
                'TasaDeExitoEnPruebas': 'first',
                'PorcentajeTareasRetrasadas': 'first',
                'PorcentajeHitosRetrasados': 'first',
                'AnioInicio': 'first',
                'MesInicio': 'first'
            })
            .round(2)
            .sort_values(['Estado', 'AnioInicio', 'MesInicio']))

def drilldown_temporal(df: pd.DataFrame, nivel: str = 'anio') -> pd.DataFrame:
    """
    Drill-down temporal: Año -> Trimestre -> Mes
    """
    if nivel == 'anio':
        return rollup_por_anio(df)
    elif nivel == 'trimestre':
        df_temp = df.copy()
        df_temp['Trimestre'] = ((df_temp['MesInicio'] - 1) // 3 + 1)
        return df_temp.groupby(['AnioInicio', 'Trimestre']).agg({
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        }).round(2)
    elif nivel == 'mes':
        return df.groupby(['AnioInicio', 'MesInicio']).agg({
            'ID_Proyecto': 'count',
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'DesviacionPresupuestal': 'sum',
            'ProductividadPromedio': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        }).round(2)

def pivot_cliente_estado(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: Clientes en filas, Estados en columnas"""
    return pd.pivot_table(
        df,
        values=['Presupuesto', 'CosteReal', 'ID_Proyecto'],
        index='CodigoClienteReal',
        columns='Estado',
        aggfunc={
            'Presupuesto': 'sum',
            'CosteReal': 'sum',
            'ID_Proyecto': 'count'
        },
        margins=True,
        fill_value=0
    )

def pivot_anio_calidad(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: Años en filas, Categorías de calidad en columnas"""
    return pd.pivot_table(
        df,
        values=['ID_Proyecto', 'TasaDeExitoEnPruebas', 'ProductividadPromedio'],
        index='AnioInicio',
        columns='CategoriaCalidad',
        aggfunc={
            'ID_Proyecto': 'count',
            'TasaDeExitoEnPruebas': 'mean',
            'ProductividadPromedio': 'mean'
        },
        margins=True,
        fill_value=0
    )

def pivot_presupuesto_productividad(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: Categoría presupuesto en filas, Categoría productividad en columnas"""
    return pd.pivot_table(
        df,
        values=['ID_Proyecto', 'DesviacionPresupuestal', 'TasaDeExitoEnPruebas'],
        index='CategoriaPresupuesto',
        columns='CategoriaProductividad',
        aggfunc={
            'ID_Proyecto': 'count',
            'DesviacionPresupuestal': 'mean',
            'TasaDeExitoEnPruebas': 'mean'
        },
        margins=True,
        fill_value=0
    )

def operacion_personalizada(df: pd.DataFrame,
                          operacion: str,
                          dimensiones: List[str],
                          medidas: List[str],
                          filtros: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Operación OLAP personalizada
    
    Args:
        df: DataFrame con los datos
        operacion: 'slice', 'dice', 'rollup', 'drilldown', 'pivot'
        dimensiones: Lista de dimensiones para la operación
        medidas: Lista de medidas para agregar
        filtros: Diccionario de filtros a aplicar
    """
    # Aplicar filtros si se especifican
    df_filtered = df.copy()
    if filtros:
        for columna, valores in filtros.items():
            if isinstance(valores, list):
                df_filtered = df_filtered[df_filtered[columna].isin(valores)]
            else:
                df_filtered = df_filtered[df_filtered[columna] == valores]
    
    # Definir agregaciones automáticas
    agregaciones = {}
    for medida in medidas:
        if any(x in medida for x in ['Presupuesto', 'Costo', 'Monto', 'Desviacion']):
            agregaciones[medida] = 'sum'
        elif any(x in medida for x in ['Porcentaje', 'Tasa', 'Productividad']):
            agregaciones[medida] = 'mean'
        elif 'ID_' in medida:
            agregaciones[medida] = 'count'
        else:
            agregaciones[medida] = 'sum'
    
    # Ejecutar operación
    if operacion == 'rollup':
        return df_filtered.groupby(dimensiones).agg(agregaciones).round(2)
    elif operacion == 'pivot' and len(dimensiones) >= 2:
        return pd.pivot_table(
            df_filtered,
            values=medidas,
            index=dimensiones[:-1],
            columns=dimensiones[-1],
            aggfunc=agregaciones,
            margins=True,
            fill_value=0
        )
    else:
        return df_filtered.groupby(dimensiones).agg(agregaciones).round(2)