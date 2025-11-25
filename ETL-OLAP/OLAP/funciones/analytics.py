#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Analytics para KPIs de Proyectos
Calcula y analiza los 11 KPIs principales del negocio
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple

class KPIAnalytics:
    """
    Clase para cálculo y análisis de KPIs de proyectos
    """
    
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        """
        Inicializa el analizador de KPIs con los dataframes del DW
        
        Args:
            dataframes: Diccionario con todos los dataframes del Data Warehouse
        """
        self.df = dataframes
        self.df_principal = dataframes.get('vista_proyectos_completa', pd.DataFrame())
        self.df_asignaciones = dataframes.get('vista_asignaciones_completa', pd.DataFrame())
        self.df_hitos_tareas = dataframes.get('vista_hitos_tareas', pd.DataFrame())
        
        # Validar datos requeridos
        if self.df_principal.empty:
            raise ValueError("No se encontraron datos en vista_proyectos_completa")
    
    def get_kpis(self) -> Dict[str, Any]:
        """
        Calcula todos los KPIs principales del negocio
        
        Returns:
            Diccionario con todos los KPIs calculados
        """
        try:
            # Preparar datos
            df = self.df_principal.copy()
            
            # Asegurar que las columnas necesarias existen
            if 'Presupuesto' not in df.columns or 'CosteReal' not in df.columns:
                raise ValueError("Faltan columnas Presupuesto o CosteReal en los datos")
            
            # 1. Cumplimiento de presupuesto (≥90%)
            df['cumplimiento_presupuesto_individual'] = np.where(
                df['Presupuesto'] > 0,
                np.minimum(1, df['Presupuesto'] / df['CosteReal']),
                0
            )
            cumplimiento_presupuesto = df['cumplimiento_presupuesto_individual'].mean() * 100
            
            # 2. Desviación presupuestal (≤5%)
            df['desviacion_porcentual'] = np.where(
                df['Presupuesto'] > 0,
                abs(df['CosteReal'] - df['Presupuesto']) / df['Presupuesto'] * 100,
                0
            )
            desviacion_presupuestal = df['desviacion_porcentual'].mean()
            
            # 3. Penalizaciones (≤2%)
            penalizaciones_porcentaje = np.where(
                df['Presupuesto'] > 0,
                df['PenalizacionesMonto'] / df['Presupuesto'] * 100,
                0
            ).mean()
            
            # 4. Proyectos a tiempo (≥85%)
            proyectos_a_tiempo = (df['RetrasoFinalDias'] <= 0).mean() * 100
            
            # 5. Proyectos cancelados (≤5%)
            proyectos_cancelados = (df['Cancelado'] == 1).mean() * 100
            
            # 6. Tareas retrasadas (≤10%)
            if not self.df_hitos_tareas.empty and 'SeRetraso' in self.df_hitos_tareas.columns:
                tareas_retrasadas = (self.df_hitos_tareas['SeRetraso'] == 1).mean() * 100
            elif 'PorcentajeTareasRetrasadas' in df.columns:
                tareas_retrasadas = df['PorcentajeTareasRetrasadas'].mean()
            else:
                tareas_retrasadas = 15.0  # Valor por defecto conservador
            
            # 7. Hitos retrasados (≤10%)
            hitos_retrasados = df['PorcentajeHitosRetrasados'].mean()
            
            # 8. Tasa de errores (≤5%)
            tasa_errores = df['TasaDeErroresEncontrados'].mean() * 100
            
            # 9. Productividad (≥75%)
            productividad = df['ProductividadPromedio'].mean()
            # Normalizar a porcentaje (asumiendo rango 0-1000 hrs, óptimo ~500)
            productividad_porcentaje = np.minimum(100, (1000 - productividad) / 1000 * 100)
            
            # 10. Éxito en pruebas (≥90%)
            exito_pruebas = df['TasaDeExitoEnPruebas'].mean() * 100
            
            # 11. Relación horas reales/planificadas (≤110%)
            if not self.df_asignaciones.empty and 'HorasPlanificadas' in self.df_asignaciones.columns:
                horas_reales_total = self.df_asignaciones['HorasReales'].sum()
                horas_planificadas_total = self.df_asignaciones['HorasPlanificadas'].sum()
                relacion_horas = (horas_reales_total / horas_planificadas_total * 100) if horas_planificadas_total > 0 else 100
            else:
                # Usar datos aproximados del dataframe principal
                relacion_horas = 105  # Valor por defecto conservador
            
            kpis = {
                'cumplimiento_presupuesto': {
                    'valor': round(cumplimiento_presupuesto, 2),
                    'objetivo': 90,
                    'unidad': '%',
                    'descripcion': 'Capacidad de ejecutar proyectos dentro del presupuesto',
                    'estado': 'bueno' if cumplimiento_presupuesto >= 90 else 'malo' if cumplimiento_presupuesto < 85 else 'regular'
                },
                'desviacion_presupuestal': {
                    'valor': round(desviacion_presupuestal, 2),
                    'objetivo': 5,
                    'unidad': '%',
                    'descripcion': 'Variación promedio entre presupuesto y costo real',
                    'estado': 'bueno' if desviacion_presupuestal <= 5 else 'malo' if desviacion_presupuestal > 10 else 'regular'
                },
                'penalizaciones': {
                    'valor': round(penalizaciones_porcentaje, 2),
                    'objetivo': 2,
                    'unidad': '%',
                    'descripcion': 'Impacto financiero de penalizaciones contractuales',
                    'estado': 'bueno' if penalizaciones_porcentaje <= 2 else 'malo' if penalizaciones_porcentaje > 5 else 'regular'
                },
                'proyectos_a_tiempo': {
                    'valor': round(proyectos_a_tiempo, 2),
                    'objetivo': 85,
                    'unidad': '%',
                    'descripcion': 'Puntualidad en la entrega de proyectos',
                    'estado': 'bueno' if proyectos_a_tiempo >= 85 else 'malo' if proyectos_a_tiempo < 70 else 'regular'
                },
                'proyectos_cancelados': {
                    'valor': round(proyectos_cancelados, 2),
                    'objetivo': 5,
                    'unidad': '%',
                    'descripcion': 'Tasa de proyectos no completados',
                    'estado': 'bueno' if proyectos_cancelados <= 5 else 'malo' if proyectos_cancelados > 10 else 'regular'
                },
                'tareas_retrasadas': {
                    'valor': round(tareas_retrasadas, 2),
                    'objetivo': 10,
                    'unidad': '%',
                    'descripcion': 'Porcentaje de tareas que no cumplen fechas',
                    'estado': 'bueno' if tareas_retrasadas <= 10 else 'malo' if tareas_retrasadas > 20 else 'regular'
                },
                'hitos_retrasados': {
                    'valor': round(hitos_retrasados, 2),
                    'objetivo': 10,
                    'unidad': '%',
                    'descripcion': 'Retrasos en hitos críticos del proyecto',
                    'estado': 'bueno' if hitos_retrasados <= 10 else 'malo' if hitos_retrasados > 20 else 'regular'
                },
                'tasa_errores': {
                    'valor': round(tasa_errores, 2),
                    'objetivo': 5,
                    'unidad': '%',
                    'descripcion': 'Proporción de errores durante desarrollo',
                    'estado': 'bueno' if tasa_errores <= 5 else 'malo' if tasa_errores > 10 else 'regular'
                },
                'productividad': {
                    'valor': round(productividad_porcentaje, 2),
                    'objetivo': 75,
                    'unidad': '%',
                    'descripcion': 'Eficiencia del equipo (valor/esfuerzo)',
                    'estado': 'bueno' if productividad_porcentaje >= 75 else 'malo' if productividad_porcentaje < 60 else 'regular'
                },
                'exito_pruebas': {
                    'valor': round(exito_pruebas, 2),
                    'objetivo': 90,
                    'unidad': '%',
                    'descripcion': 'Efectividad del proceso de testing',
                    'estado': 'bueno' if exito_pruebas >= 90 else 'malo' if exito_pruebas < 80 else 'regular'
                },
                'relacion_horas': {
                    'valor': round(relacion_horas, 2),
                    'objetivo': 110,
                    'unidad': '%',
                    'descripcion': 'Precisión en estimación de esfuerzo',
                    'estado': 'bueno' if relacion_horas <= 110 else 'malo' if relacion_horas > 130 else 'regular'
                }
            }
            
            return kpis
            
        except Exception as e:
            print(f"[ERROR] Error calculando KPIs: {e}")
            return {}
    
    def get_kpis_por_cliente(self) -> pd.DataFrame:
        """
        Calcula KPIs agrupados por cliente
        
        Returns:
            DataFrame con KPIs por cliente
        """
        try:
            df = self.df_principal.copy()
            
            # Calcular columnas necesarias si no existen
            if 'cumplimiento_presupuesto_individual' not in df.columns:
                df['cumplimiento_presupuesto_individual'] = np.where(
                    df['Presupuesto'] > 0,
                    np.minimum(1, df['Presupuesto'] / df['CosteReal']),
                    0
                )
            
            if 'desviacion_porcentual' not in df.columns:
                df['desviacion_porcentual'] = np.where(
                    df['Presupuesto'] > 0,
                    abs(df['CosteReal'] - df['Presupuesto']) / df['Presupuesto'] * 100,
                    0
                )
            
            kpis_cliente = df.groupby('CodigoClienteReal').agg({
                'cumplimiento_presupuesto_individual': 'mean',
                'desviacion_porcentual': 'mean',
                'PenalizacionesMonto': 'sum',
                'Presupuesto': 'sum',
                'RetrasoFinalDias': lambda x: (x <= 0).mean(),
                'Cancelado': lambda x: (x == 1).mean(),
                'PorcentajeTareasRetrasadas': 'mean',
                'PorcentajeHitosRetrasados': 'mean',
                'TasaDeErroresEncontrados': 'mean',
                'ProductividadPromedio': 'mean',
                'TasaDeExitoEnPruebas': 'mean',
                'ID_Proyecto': 'count'
            }).round(2)
            
            # Renombrar columnas
            kpis_cliente.columns = [
                'cumplimiento_presupuesto', 'desviacion_presupuestal', 
                'penalizaciones_total', 'presupuesto_total',
                'proyectos_a_tiempo', 'proyectos_cancelados',
                'tareas_retrasadas', 'hitos_retrasados',
                'tasa_errores', 'productividad', 'exito_pruebas',
                'total_proyectos'
            ]
            
            # Calcular penalizaciones como porcentaje
            kpis_cliente['penalizaciones_porcentaje'] = (
                kpis_cliente['penalizaciones_total'] / kpis_cliente['presupuesto_total'] * 100
            ).fillna(0)
            
            return kpis_cliente
            
        except Exception as e:
            print(f"[ERROR] Error calculando KPIs por cliente: {e}")
            return pd.DataFrame()
    
    def get_tendencias_temporales(self) -> Dict[str, pd.DataFrame]:
        """
        Calcula tendencias de KPIs a lo largo del tiempo
        
        Returns:
            Diccionario con tendencias por año y mes
        """
        try:
            df = self.df_principal.copy()
            
            # Tendencias anuales
            tendencias_anuales = df.groupby('AnioInicio').agg({
                'cumplimiento_presupuesto_individual': 'mean',
                'desviacion_porcentual': 'mean',
                'RetrasoFinalDias': lambda x: (x <= 0).mean(),
                'Cancelado': lambda x: (x == 1).mean(),
                'TasaDeExitoEnPruebas': 'mean',
                'ProductividadPromedio': 'mean',
                'ID_Proyecto': 'count'
            }).round(2)
            
            # Tendencias mensuales (últimos 12 meses)
            df['periodo'] = df['AnioInicio'].astype(str) + '-' + df['MesInicio'].astype(str).str.zfill(2)
            tendencias_mensuales = df.groupby('periodo').agg({
                'cumplimiento_presupuesto_individual': 'mean',
                'desviacion_porcentual': 'mean',
                'RetrasoFinalDias': lambda x: (x <= 0).mean(),
                'TasaDeExitoEnPruebas': 'mean',
                'ID_Proyecto': 'count'
            }).round(2).tail(12)
            
            return {
                'anuales': tendencias_anuales,
                'mensuales': tendencias_mensuales
            }
            
        except Exception as e:
            print(f"[ERROR] Error calculando tendencias: {e}")
            return {}
    
    def get_alertas_kpis(self) -> List[Dict[str, Any]]:
        """
        Genera alertas basadas en KPIs fuera de los umbrales
        
        Returns:
            Lista de alertas con información detallada
        """
        kpis = self.get_kpis()
        alertas = []
        
        for nombre, kpi in kpis.items():
            if kpi['estado'] == 'malo':
                alertas.append({
                    'kpi': nombre,
                    'valor': kpi['valor'],
                    'objetivo': kpi['objetivo'],
                    'unidad': kpi['unidad'],
                    'descripcion': kpi['descripcion'],
                    'severidad': 'alta',
                    'recomendacion': self._get_recomendacion(nombre, kpi['valor'], kpi['objetivo'])
                })
            elif kpi['estado'] == 'regular':
                alertas.append({
                    'kpi': nombre,
                    'valor': kpi['valor'],
                    'objetivo': kpi['objetivo'],
                    'unidad': kpi['unidad'],
                    'descripcion': kpi['descripcion'],
                    'severidad': 'media',
                    'recomendacion': self._get_recomendacion(nombre, kpi['valor'], kpi['objetivo'])
                })
        
        return alertas
    
    def _get_recomendacion(self, kpi: str, valor: float, objetivo: float) -> str:
        """
        Genera recomendaciones específicas por KPI
        """
        recomendaciones = {
            'cumplimiento_presupuesto': 'Revisar proceso de estimación y control de costos',
            'desviacion_presupuestal': 'Mejorar precisión en estimaciones iniciales',
            'penalizaciones': 'Fortalecer gestión de riesgos y cumplimiento contractual',
            'proyectos_a_tiempo': 'Optimizar planificación temporal y gestión de dependencias',
            'proyectos_cancelados': 'Revisar criterios de selección y viabilidad de proyectos',
            'tareas_retrasadas': 'Mejorar seguimiento y gestión de tareas operativas',
            'hitos_retrasados': 'Reforzar coordinación entre equipos y entregables clave',
            'tasa_errores': 'Implementar mejores prácticas de desarrollo y revisión de código',
            'productividad': 'Optimizar asignación de recursos y eliminar bloqueos',
            'exito_pruebas': 'Fortalecer proceso de testing y control de calidad',
            'relacion_horas': 'Mejorar técnicas de estimación de esfuerzo'
        }
        
        return recomendaciones.get(kpi, 'Revisar procesos relacionados con este KPI')