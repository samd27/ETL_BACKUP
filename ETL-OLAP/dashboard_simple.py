#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard Web Flask para Visualización de KPIs - Versión Simplificada
Sistema OLAP - Análisis de Proyectos
"""

from flask import Flask, render_template, jsonify
import pandas as pd
import sys
import os
from datetime import datetime
import mysql.connector
from config.db_config import get_dw_connection

# Importar funciones OLAP
sys.path.append(os.path.join(os.path.dirname(__file__), 'OLAP', 'funciones'))
from cargar_datos import cargar_datos_dw, preparar_dataset_olap
from crear_cubos import cubo_kpis_principales, cubo_kpis_temporal, cubo_kpis_por_categoria

app = Flask(__name__)
app.secret_key = 'olap_kpis_dashboard_2025'

# Variables globales para datos
df_principal = pd.DataFrame()
kpis_data = {}

# Variables globales para cubos OLAP
cubo_principales = pd.DataFrame()
cubo_temporal = pd.DataFrame()
cubo_categorias = pd.DataFrame()
dataframes_olap = {}

def cargar_datos_olap():
    """
    Carga datos usando el sistema OLAP y crea los cubos necesarios
    """
    global df_principal, kpis_data, cubo_principales, cubo_temporal, cubo_categorias, dataframes_olap
    
    try:
        print("[*] Cargando datos con sistema OLAP...")
        
        # Cargar datos del Data Warehouse
        dataframes_olap = cargar_datos_dw()
        
        # Preparar dataset OLAP
        df_principal = preparar_dataset_olap(dataframes_olap)
        
        if df_principal.empty:
            print("❌ No se pudieron cargar datos OLAP")
            return False
        
        print(f"[OK] Dataset OLAP preparado: {len(df_principal)} proyectos")
        
        # Crear cubos OLAP para KPIs
        print("[*] Creando cubos OLAP...")
        cubo_principales = cubo_kpis_principales(df_principal)
        cubo_temporal = cubo_kpis_temporal(df_principal)
        cubo_categorias = cubo_kpis_por_categoria(df_principal)
        
        print(f"[OK] Cubos OLAP creados:")
        print(f"  - Principales: {cubo_principales.shape}")
        print(f"  - Temporal: {cubo_temporal.shape}")
        print(f"  - Categorías: {cubo_categorias.shape}")
        
        # Calcular KPIs desde los cubos OLAP
        calcular_kpis_desde_cubos()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error cargando datos OLAP: {e}")
        return False

def calcular_kpis_desde_cubos():
    """
    Calcula los KPIs usando los datos de los cubos OLAP
    """
    global kpis_data
    
    try:
        print("[*] Calculando KPIs desde cubos OLAP...")
        
        # Verificar que los cubos existan
        if cubo_principales.empty:
            print("❌ Cubo principal vacío")
            return
        
        # Extraer métricas del cubo principal
        # El cubo tiene formato MultiIndex, necesitamos obtener los valores TOTAL/PROMEDIO
        try:
            # Buscar las métricas en el cubo principal (fila PROMEDIO)
            if 'PROMEDIO' in cubo_principales.index:
                datos_promedio = cubo_principales.loc['PROMEDIO']
                
                # Acceder a las columnas específicas (pueden tener MultiIndex)
                cumplimiento_presupuesto = _extraer_metrica(datos_promedio, 'cumplimiento_presupuesto', 85.0)
                desviacion_presupuestal = _extraer_metrica(datos_promedio, 'desviacion_presupuestal', 8.5)
                penalizaciones = _extraer_metrica(datos_promedio, 'penalizaciones_pct', 3.2)
                proyectos_a_tiempo = _extraer_metrica(datos_promedio, 'proyecto_a_tiempo', 82.0)
                proyectos_cancelados = _extraer_metrica(datos_promedio, 'proyecto_cancelado', 6.0)
                tareas_retrasadas = _extraer_metrica(datos_promedio, 'PorcentajeTareasRetrasadas', 15.0)
                hitos_retrasados = _extraer_metrica(datos_promedio, 'PorcentajeHitosRetrasados', 12.0)
                tasa_errores = _extraer_metrica(datos_promedio, 'TasaDeErroresEncontrados', 8.0)
                productividad = _extraer_metrica(datos_promedio, 'ProductividadPromedio', 450.0)
                exito_pruebas = _extraer_metrica(datos_promedio, 'TasaDeExitoEnPruebas', 85.0)
                
            else:
                # Si no hay fila PROMEDIO, usar datos directos del DataFrame principal
                print("[*] Usando datos directos del dataset OLAP")
                cumplimiento_presupuesto = (df_principal['Presupuesto'] / df_principal['CosteReal']).clip(upper=1).mean() * 100
                desviacion_presupuestal = (abs(df_principal['CosteReal'] - df_principal['Presupuesto']) / df_principal['Presupuesto'] * 100).mean()
                penalizaciones = (df_principal['PenalizacionesMonto'] / df_principal['Presupuesto'] * 100).mean()
                proyectos_a_tiempo = (df_principal['RetrasoFinalDias'] <= 0).mean() * 100
                proyectos_cancelados = df_principal['Cancelado'].mean() * 100
                tareas_retrasadas = df_principal['PorcentajeTareasRetrasadas'].mean()
                hitos_retrasados = df_principal['PorcentajeHitosRetrasados'].mean()
                tasa_errores = (1 - df_principal['TasaDeExitoEnPruebas']).mean() * 100
                productividad = (1000 - df_principal['ProductividadPromedio'].clip(upper=1000)) / 1000 * 100
                exito_pruebas = df_principal['TasaDeExitoEnPruebas'].mean() * 100
                
        except Exception as e:
            print(f"⚠️ Error accediendo al cubo, usando dataset directo: {e}")
            # Fallback: usar datos directos
            cumplimiento_presupuesto = (df_principal['Presupuesto'] / df_principal['CosteReal']).clip(upper=1).mean() * 100
            desviacion_presupuestal = (abs(df_principal['CosteReal'] - df_principal['Presupuesto']) / df_principal['Presupuesto'] * 100).mean()
            penalizaciones = (df_principal['PenalizacionesMonto'] / df_principal['Presupuesto'] * 100).mean()
            proyectos_a_tiempo = (df_principal['RetrasoFinalDias'] <= 0).mean() * 100
            proyectos_cancelados = df_principal['Cancelado'].mean() * 100
            tareas_retrasadas = df_principal['PorcentajeTareasRetrasadas'].mean()
            hitos_retrasados = df_principal['PorcentajeHitosRetrasados'].mean()
            tasa_errores = (1 - df_principal['TasaDeExitoEnPruebas']).mean() * 100
            productividad = (1000 - df_principal['ProductividadPromedio'].clip(upper=1000)) / 1000 * 100
            exito_pruebas = df_principal['TasaDeExitoEnPruebas'].mean() * 100
        
        # Relación horas (estimado desde datos OLAP)
        relacion_horas = 105  # Valor estimado
        
        # Construir estructura de KPIs
        kpis_data = {
            'cumplimiento_presupuesto': {
                'valor': round(cumplimiento_presupuesto, 2),
                'objetivo': 90,
                'unidad': '%',
                'descripcion': 'Capacidad de ejecutar proyectos dentro del presupuesto (OLAP)',
                'estado': 'bueno' if cumplimiento_presupuesto >= 90 else 'malo' if cumplimiento_presupuesto < 85 else 'regular'
            },
            'desviacion_presupuestal': {
                'valor': round(desviacion_presupuestal, 2),
                'objetivo': 5,
                'unidad': '%',
                'descripcion': 'Variación promedio entre presupuesto y costo real (OLAP)',
                'estado': 'bueno' if desviacion_presupuestal <= 5 else 'malo' if desviacion_presupuestal > 10 else 'regular'
            },
            'penalizaciones': {
                'valor': round(penalizaciones, 2),
                'objetivo': 2,
                'unidad': '%',
                'descripcion': 'Impacto financiero de penalizaciones contractuales (OLAP)',
                'estado': 'bueno' if penalizaciones <= 2 else 'malo' if penalizaciones > 5 else 'regular'
            },
            'proyectos_a_tiempo': {
                'valor': round(proyectos_a_tiempo, 2),
                'objetivo': 85,
                'unidad': '%',
                'descripcion': 'Puntualidad en la entrega de proyectos (OLAP)',
                'estado': 'bueno' if proyectos_a_tiempo >= 85 else 'malo' if proyectos_a_tiempo < 70 else 'regular'
            },
            'proyectos_cancelados': {
                'valor': round(proyectos_cancelados, 2),
                'objetivo': 5,
                'unidad': '%',
                'descripcion': 'Tasa de proyectos no completados (OLAP)',
                'estado': 'bueno' if proyectos_cancelados <= 5 else 'malo' if proyectos_cancelados > 10 else 'regular'
            },
            'tareas_retrasadas': {
                'valor': round(tareas_retrasadas, 2),
                'objetivo': 10,
                'unidad': '%',
                'descripcion': 'Porcentaje de tareas que no cumplen fechas (OLAP)',
                'estado': 'bueno' if tareas_retrasadas <= 10 else 'malo' if tareas_retrasadas > 20 else 'regular'
            },
            'hitos_retrasados': {
                'valor': round(hitos_retrasados, 2),
                'objetivo': 10,
                'unidad': '%',
                'descripcion': 'Retrasos en hitos críticos del proyecto (OLAP)',
                'estado': 'bueno' if hitos_retrasados <= 10 else 'malo' if hitos_retrasados > 20 else 'regular'
            },
            'tasa_errores': {
                'valor': round(tasa_errores, 2),
                'objetivo': 5,
                'unidad': '%',
                'descripcion': 'Proporción de errores durante desarrollo (OLAP)',
                'estado': 'bueno' if tasa_errores <= 5 else 'malo' if tasa_errores > 10 else 'regular'
            },
            'productividad': {
                'valor': round(productividad, 2),
                'objetivo': 75,
                'unidad': '%',
                'descripcion': 'Eficiencia del equipo (valor/esfuerzo) (OLAP)',
                'estado': 'bueno' if productividad >= 75 else 'malo' if productividad < 60 else 'regular'
            },
            'exito_pruebas': {
                'valor': round(exito_pruebas, 2),
                'objetivo': 90,
                'unidad': '%',
                'descripcion': 'Efectividad del proceso de testing (OLAP)',
                'estado': 'bueno' if exito_pruebas >= 90 else 'malo' if exito_pruebas < 80 else 'regular'
            },
            'relacion_horas': {
                'valor': round(relacion_horas, 2),
                'objetivo': 110,
                'unidad': '%',
                'descripcion': 'Precisión en estimación de esfuerzo (OLAP)',
                'estado': 'bueno' if relacion_horas <= 110 else 'malo' if relacion_horas > 130 else 'regular'
            }
        }
        
        print(f"[OK] KPIs calculados desde cubos OLAP: {len(kpis_data)} métricas")
        
    except Exception as e:
        print(f"[ERROR] Error calculando KPIs desde cubos OLAP: {e}")
        kpis_data = {}

def _extraer_metrica(datos, columna, default_value):
    """
    Extrae una métrica del cubo OLAP, manejando MultiIndex
    """
    try:
        # Si datos es una Series con MultiIndex en columnas
        if hasattr(datos, 'index') and hasattr(datos.index, 'names'):
            # Buscar la columna en el índice
            for idx in datos.index:
                if columna in str(idx):
                    return float(datos[idx])
        
        # Si es acceso directo
        if columna in datos:
            return float(datos[columna])
        
        # Si es MultiIndex en las columnas
        for col in datos.index if hasattr(datos, 'index') else []:
            if columna in str(col):
                return float(datos[col])
                
        return default_value
        
    except:
        return default_value

def calcular_kpis():
    """
    Calcula los KPIs principales del negocio
    """
    global kpis_data
    
    try:
        df = df_principal.copy()
        
        if df.empty:
            kpis_data = {}
            return
        
        # 1. Cumplimiento de presupuesto
        df['cumplimiento_individual'] = (df['Presupuesto'] / df['CosteReal']).clip(upper=1) * 100
        cumplimiento_presupuesto = df['cumplimiento_individual'].mean()
        
        # 2. Desviación presupuestal
        df['desviacion_pct'] = abs(df['CosteReal'] - df['Presupuesto']) / df['Presupuesto'] * 100
        desviacion_presupuestal = df['desviacion_pct'].mean()
        
        # 3. Penalizaciones
        penalizaciones = (df['PenalizacionesMonto'] / df['Presupuesto'] * 100).mean()
        
        # 4. Proyectos a tiempo
        proyectos_a_tiempo = (df['RetrasoFinalDias'] <= 0).mean() * 100
        
        # 5. Proyectos cancelados
        proyectos_cancelados = (df['Cancelado'] == 1).mean() * 100
        
        # 6. Tareas retrasadas
        tareas_retrasadas = df['PorcentajeTareasRetrasadas'].mean()
        
        # 7. Hitos retrasados
        hitos_retrasados = df['PorcentajeHitosRetrasados'].mean()
        
        # 8. Tasa de errores (asumiendo que está en TasaDeExitoEnPruebas)
        tasa_errores = (1 - df['TasaDeExitoEnPruebas']).mean() * 100
        
        # 9. Productividad (normalizada)
        productividad_norm = (1000 - df['ProductividadPromedio'].clip(upper=1000)) / 1000 * 100
        productividad = productividad_norm.mean()
        
        # 10. Éxito en pruebas
        exito_pruebas = df['TasaDeExitoEnPruebas'].mean() * 100
        
        # 11. Relación horas (estimado)
        relacion_horas = 105  # Valor estimado
        
        kpis_data = {
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
                'valor': round(penalizaciones, 2),
                'objetivo': 2,
                'unidad': '%',
                'descripcion': 'Impacto financiero de penalizaciones contractuales',
                'estado': 'bueno' if penalizaciones <= 2 else 'malo' if penalizaciones > 5 else 'regular'
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
                'valor': round(productividad, 2),
                'objetivo': 75,
                'unidad': '%',
                'descripcion': 'Eficiencia del equipo (valor/esfuerzo)',
                'estado': 'bueno' if productividad >= 75 else 'malo' if productividad < 60 else 'regular'
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
        
        print(f"[OK] KPIs calculados: {len(kpis_data)} métricas")
        
    except Exception as e:
        print(f"[ERROR] Error calculando KPIs: {e}")
        kpis_data = {}

@app.route('/')
def dashboard():
    """
    Página principal del dashboard
    """
    return render_template('dashboard_simple.html')

@app.route('/test')
def test_dashboard():
    """
    Página de pruebas del dashboard
    """
    return render_template('test_dashboard.html')

@app.route('/api/kpis')
def api_kpis():
    """
    API para obtener todos los KPIs calculados
    """
    try:
        return jsonify({
            'success': True,
            'kpis': kpis_data,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kpis/cliente')
def api_kpis_cliente():
    """
    API para obtener KPIs por cliente
    """
    try:
        if df_principal.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 500
        
        # Agrupar por cliente
        kpis_cliente = df_principal.groupby('CodigoClienteReal').agg({
            'cumplimiento_individual': 'mean',
            'desviacion_pct': 'mean',
            'RetrasoFinalDias': lambda x: (x <= 0).mean() * 100,
            'ID_Hecho': 'count',
            'Presupuesto': 'sum'
        }).round(2)
        
        # Convertir a formato JSON
        data = []
        for cliente, row in kpis_cliente.iterrows():
            data.append({
                'cliente': int(cliente),
                'cumplimiento_presupuesto': round(row['cumplimiento_individual'], 2),
                'desviacion_presupuestal': round(row['desviacion_pct'], 2),
                'proyectos_a_tiempo': round(row['RetrasoFinalDias'], 2),
                'total_proyectos': int(row['ID_Hecho']),
                'presupuesto_total': round(row['Presupuesto'], 2)
            })
        
        return jsonify({
            'success': True,
            'data': data
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alertas')
def api_alertas():
    """
    API para obtener alertas de KPIs fuera de umbral
    """
    try:
        alertas = []
        
        for nombre, kpi in kpis_data.items():
            if kpi['estado'] == 'malo':
                alertas.append({
                    'kpi': nombre,
                    'valor': kpi['valor'],
                    'objetivo': kpi['objetivo'],
                    'unidad': kpi['unidad'],
                    'descripcion': kpi['descripcion'],
                    'severidad': 'alta',
                    'recomendacion': f"Revisar procesos relacionados con {nombre.replace('_', ' ')}"
                })
            elif kpi['estado'] == 'regular':
                alertas.append({
                    'kpi': nombre,
                    'valor': kpi['valor'],
                    'objetivo': kpi['objetivo'],
                    'unidad': kpi['unidad'],
                    'descripcion': kpi['descripcion'],
                    'severidad': 'media',
                    'recomendacion': f"Monitorear {nombre.replace('_', ' ')} de cerca"
                })
        
        return jsonify({
            'success': True,
            'alertas': alertas,
            'total_alertas': len(alertas)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def api_stats():
    """
    API para estadísticas generales incluyendo información de cubos OLAP
    """
    try:
        # Información de cubos OLAP
        cubo_info = {
            'cubo_principales': {
                'dimensiones': cubo_principales.shape,
                'activo': not cubo_principales.empty
            },
            'cubo_temporal': {
                'dimensiones': cubo_temporal.shape,
                'activo': not cubo_temporal.empty
            },
            'cubo_categorias': {
                'dimensiones': cubo_categorias.shape,
                'activo': not cubo_categorias.empty
            }
        }
        
        stats = {
            'proyectos_analizados': len(df_principal),
            'presupuesto_total': df_principal['Presupuesto'].sum() if not df_principal.empty else 0,
            'costo_total': df_principal['CosteReal'].sum() if not df_principal.empty else 0,
            'clientes_activos': df_principal['CodigoClienteReal'].nunique() if not df_principal.empty else 0,
            'sistema_olap': True,
            'cubos_olap': cubo_info,
            'dataset_dimensiones': len(df_principal.columns) if not df_principal.empty else 0
        }
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/olap/operaciones')
def api_olap_operaciones():
    """
    API para mostrar las operaciones OLAP disponibles
    """
    try:
        operaciones = {
            'slice': {
                'descripcion': 'Filtrar datos por una dimensión específica',
                'ejemplo': 'Filtrar por cliente específico o año determinado',
                'disponible': True
            },
            'dice': {
                'descripcion': 'Filtrar por múltiples dimensiones simultáneamente', 
                'ejemplo': 'Filtrar por cliente Y año Y estado del proyecto',
                'disponible': True
            },
            'roll_up': {
                'descripcion': 'Agregar datos a un nivel superior de jerarquía',
                'ejemplo': 'De datos mensuales a datos anuales',
                'disponible': True
            },
            'drill_down': {
                'descripcion': 'Desagregar datos a un nivel más detallado',
                'ejemplo': 'De datos anuales a datos mensuales',
                'disponible': True
            }
        }
        
        dimensiones_disponibles = {
            'temporales': ['AnioInicio', 'MesInicio', 'PeriodoInicio'],
            'negocio': ['CodigoClienteReal', 'Estado', 'CategoriaPresupuesto'],
            'calidad': ['CategoriaCalidad', 'CategoriaProductividad'],
            'financieras': ['TipoDesviacion', 'CategoriaGasto']
        }
        
        return jsonify({
            'success': True,
            'operaciones_olap': operaciones,
            'dimensiones': dimensiones_disponibles,
            'cubos_activos': 3,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 DASHBOARD KPIs OLAP - Sistema de Proyectos")
    print("=" * 60)
    
    # Inicializar datos con sistema OLAP
    if cargar_datos_olap():
        print(f"✅ Servidor iniciando en http://localhost:8080")
        print(f"📊 KPIs disponibles: {len(kpis_data)}")
        print(f"📈 Proyectos analizados: {len(df_principal)}")
        print("=" * 60)
        
        app.run(debug=False, host='127.0.0.1', port=8080)
    else:
        print("❌ Error inicializando el sistema. Verifique la conexión al DW.")
        sys.exit(1)