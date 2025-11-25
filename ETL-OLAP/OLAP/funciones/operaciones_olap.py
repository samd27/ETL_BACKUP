#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Operaciones OLAP Reales
Implementa las 5 operaciones fundamentales del OLAP:
- SLICE: Filtrar por una dimensión
- DICE: Filtrar por múltiples dimensiones  
- ROLL-UP: Agregar subiendo en jerarquías
- DRILL-DOWN: Desglosar bajando en jerarquías
- PIVOT: Rotar dimensiones del cubo
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional

class CuboOLAP:
    """
    Clase que representa un cubo OLAP con operaciones reales
    """
    
    def __init__(self, datos: pd.DataFrame, dimensiones: List[str], medidas: List[str]):
        """
        Inicializa el cubo OLAP
        
        Args:
            datos: DataFrame con los datos
            dimensiones: Lista de columnas que son dimensiones
            medidas: Lista de columnas que son medidas
        """
        self.datos = datos.copy()
        self.dimensiones = dimensiones
        self.medidas = medidas
        self.cubo_base = None  # Se crea bajo demanda
    
    def _crear_cubo_base(self) -> pd.DataFrame:
        """
        Crea la estructura base del cubo con todas las combinaciones (solo cuando se necesite)
        """
        if self.cubo_base is not None:
            return self.cubo_base
            
        try:
            print(f"[*] Creando cubo base con {len(self.dimensiones)} dimensiones y {len(self.medidas)} medidas...")
            self.cubo_base = self.datos.groupby(self.dimensiones)[self.medidas].agg({
                medida: ['sum', 'mean', 'count'] for medida in self.medidas  # Reducir agregaciones
            }).fillna(0)
            print(f"[OK] Cubo base creado: {self.cubo_base.shape}")
            return self.cubo_base
        except Exception as e:
            print(f"[ERROR] Error creando cubo base: {e}")
            # Crear cubo simple si falla
            self.cubo_base = self.datos[self.medidas].agg(['sum', 'mean', 'count'])
            return self.cubo_base
    
    def slice(self, dimension: str, valor: Any) -> pd.DataFrame:
        """
        SLICE: Filtra el cubo por un valor específico de una dimensión
        
        Args:
            dimension: Nombre de la dimensión
            valor: Valor por el que filtrar
            
        Returns:
            DataFrame filtrado
        """
        if dimension not in self.dimensiones:
            raise ValueError(f"Dimensión '{dimension}' no existe en el cubo")
        
        filtro = self.datos[dimension] == valor
        datos_filtrados = self.datos[filtro]
        
        if datos_filtrados.empty:
            return pd.DataFrame()
        
        # Crear cubo sin la dimensión del slice
        dims_restantes = [d for d in self.dimensiones if d != dimension]
        
        if not dims_restantes:
            # Si no quedan dimensiones, agregar todas las medidas
            return datos_filtrados[self.medidas].agg(['sum', 'mean', 'count', 'min', 'max'])
        
        return datos_filtrados.groupby(dims_restantes)[self.medidas].agg({
            medida: ['sum', 'mean', 'count'] for medida in self.medidas
        }).fillna(0)
    
    def dice(self, filtros: Dict[str, Any]) -> pd.DataFrame:
        """
        DICE: Filtra el cubo por múltiples dimensiones
        
        Args:
            filtros: Diccionario {dimension: valor} con los filtros
            
        Returns:
            DataFrame filtrado
        """
        datos_filtrados = self.datos.copy()
        
        for dimension, valor in filtros.items():
            if dimension not in self.dimensiones:
                raise ValueError(f"Dimensión '{dimension}' no existe en el cubo")
            
            if isinstance(valor, list):
                datos_filtrados = datos_filtrados[datos_filtrados[dimension].isin(valor)]
            else:
                datos_filtrados = datos_filtrados[datos_filtrados[dimension] == valor]
        
        if datos_filtrados.empty:
            return pd.DataFrame()
        
        # Crear cubo con las dimensiones que no están en los filtros
        dims_restantes = [d for d in self.dimensiones if d not in filtros.keys()]
        
        if not dims_restantes:
            return datos_filtrados[self.medidas].agg(['sum', 'mean', 'count', 'min', 'max'])
        
        return datos_filtrados.groupby(dims_restantes)[self.medidas].agg({
            medida: ['sum', 'mean', 'count'] for medida in self.medidas
        }).fillna(0)
    
    def roll_up(self, dimension: str, jerarquia: List[str]) -> pd.DataFrame:
        """
        ROLL-UP: Agrega datos subiendo en la jerarquía
        
        Args:
            dimension: Dimensión sobre la que hacer roll-up
            jerarquia: Lista ordenada de niveles (de más específico a más general)
            
        Returns:
            DataFrame con datos agregados por nivel
        """
        if dimension not in self.dimensiones:
            raise ValueError(f"Dimensión '{dimension}' no existe en el cubo")
        
        resultados = {}
        
        for i, nivel in enumerate(jerarquia):
            # Crear una nueva columna con el nivel actual
            if nivel == 'TOTAL':
                # Nivel más alto: agregar todo
                nivel_datos = self.datos.copy()
                nivel_datos[f'{dimension}_nivel'] = 'TOTAL'
            else:
                # Agrupar por el nivel especificado
                nivel_datos = self.datos.copy()
                if nivel in self.datos.columns:
                    nivel_datos[f'{dimension}_nivel'] = nivel_datos[nivel]
                else:
                    # Si no existe la columna, usar la dimensión original
                    nivel_datos[f'{dimension}_nivel'] = nivel_datos[dimension]
            
            # Agregar por el nivel actual y otras dimensiones
            dims_agrupacion = [d for d in self.dimensiones if d != dimension] + [f'{dimension}_nivel']
            
            nivel_resultado = nivel_datos.groupby(dims_agrupacion)[self.medidas].agg({
                medida: ['sum', 'mean', 'count'] for medida in self.medidas
            }).fillna(0)
            
            resultados[f'Nivel_{i+1}_{nivel}'] = nivel_resultado
        
        return resultados
    
    def drill_down(self, dimension: str, valor_padre: Any, dimension_hija: str) -> pd.DataFrame:
        """
        DRILL-DOWN: Desglosar datos bajando en la jerarquía
        
        Args:
            dimension: Dimensión padre
            valor_padre: Valor específico del padre
            dimension_hija: Dimensión más detallada
            
        Returns:
            DataFrame con datos desglosados
        """
        if dimension not in self.dimensiones:
            raise ValueError(f"Dimensión '{dimension}' no existe en el cubo")
        
        if dimension_hija not in self.datos.columns:
            raise ValueError(f"Dimensión hija '{dimension_hija}' no existe en los datos")
        
        # Filtrar por el valor padre
        datos_filtrados = self.datos[self.datos[dimension] == valor_padre]
        
        if datos_filtrados.empty:
            return pd.DataFrame()
        
        # Crear nuevas dimensiones incluyendo la dimensión hija
        dims_nuevas = [d for d in self.dimensiones if d != dimension] + [dimension_hija]
        
        return datos_filtrados.groupby(dims_nuevas)[self.medidas].agg({
            medida: ['sum', 'mean', 'count'] for medida in self.medidas
        }).fillna(0)
    
    def pivot(self, dim_filas: List[str], dim_columnas: List[str], medida: str, 
              agregacion: str = 'sum') -> pd.DataFrame:
        """
        PIVOT: Rota las dimensiones del cubo
        
        Args:
            dim_filas: Dimensiones para las filas
            dim_columnas: Dimensiones para las columnas
            medida: Medida a mostrar
            agregacion: Tipo de agregación ('sum', 'mean', 'count', etc.)
            
        Returns:
            DataFrame pivoteado
        """
        if medida not in self.medidas:
            raise ValueError(f"Medida '{medida}' no existe en el cubo")
        
        for dim in dim_filas + dim_columnas:
            if dim not in self.dimensiones:
                raise ValueError(f"Dimensión '{dim}' no existe en el cubo")
        
        return pd.pivot_table(
            self.datos,
            values=medida,
            index=dim_filas,
            columns=dim_columnas,
            aggfunc=agregacion,
            fill_value=0,
            margins=True,
            margins_name='TOTAL',
            observed=False
        )
    
    def get_info(self) -> Dict[str, Any]:
        """
        Obtiene información del cubo OLAP
        """
        return {
            'dimensiones': self.dimensiones,
            'medidas': self.medidas,
            'num_registros': len(self.datos),
            'shape_cubo': (0, 0) if self.cubo_base is None else self.cubo_base.shape,
            'dimensiones_valores': {
                dim: self.datos[dim].nunique() for dim in self.dimensiones
            }
        }

def crear_cubo_olap_proyectos(df: pd.DataFrame) -> CuboOLAP:
    """
    Crea un cubo OLAP para análisis de proyectos
    """
    print(f"[*] Creando cubo OLAP proyectos con {len(df)} registros...")
    
    dimensiones = [
        'CodigoClienteReal', 'Estado', 'AnioInicio', 'MesInicio',
        'CategoriaPresupuesto', 'TipoDesviacion', 'CategoriaProductividad', 
        'CategoriaCalidad', 'PeriodoInicio'
    ]
    
    medidas = [
        'Presupuesto', 'CosteReal', 'DesviacionPresupuestal',
        'ProductividadPromedio', 'TasaDeExitoEnPruebas',
        'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
    ]
    
    # Filtrar solo las columnas que existen
    dimensiones_existentes = [d for d in dimensiones if d in df.columns]
    medidas_existentes = [m for m in medidas if m in df.columns]
    
    print(f"[*] Dimensiones encontradas: {len(dimensiones_existentes)} de {len(dimensiones)}")
    print(f"[*] Medidas encontradas: {len(medidas_existentes)} de {len(medidas)}")
    
    cubo = CuboOLAP(df, dimensiones_existentes, medidas_existentes)
    print(f"[OK] Cubo OLAP proyectos creado exitosamente")
    return cubo

def crear_cubo_olap_kpis(df: pd.DataFrame) -> CuboOLAP:
    """
    Crea un cubo OLAP especializado para KPIs
    """
    print(f"[*] Creando cubo OLAP KPIs con {len(df)} registros...")
    
    dimensiones = [
        'Estado', 'CategoriaPresupuesto', 'CategoriaProductividad', 
        'CategoriaCalidad', 'AnioInicio', 'PeriodoInicio'
    ]
    
    medidas = [
        'Presupuesto', 'CosteReal', 'DesviacionPresupuestal',
        'ProductividadPromedio', 'TasaDeExitoEnPruebas',
        'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados',
        'TasaDeErroresEncontrados', 'PenalizacionesMonto'
    ]
    
    # Filtrar solo las columnas que existen
    dimensiones_existentes = [d for d in dimensiones if d in df.columns]
    medidas_existentes = [m for m in medidas if m in df.columns]
    
    print(f"[*] Dimensiones encontradas: {len(dimensiones_existentes)} de {len(dimensiones)}")
    print(f"[*] Medidas encontradas: {len(medidas_existentes)} de {len(medidas)}")
    
    cubo = CuboOLAP(df, dimensiones_existentes, medidas_existentes)
    print(f"[OK] Cubo OLAP KPIs creado exitosamente")
    return cubo