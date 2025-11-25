#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard Web Flask para Visualización de KPIs
Sistema OLAP - Análisis de Proyectos
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
import sys
import os
from datetime import datetime

# Agregar el directorio del proyecto al path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from olap.funciones.cargar_datos import cargar_datos_dw
from olap.funciones.analytics import KPIAnalytics
from olap.funciones.crear_cubos import (
    cubo_kpis_principales, cubo_kpis_temporal, cubo_kpis_por_categoria
)

app = Flask(__name__)
app.secret_key = 'olap_kpis_dashboard_2025'

# Variables globales para datos
dataframes = {}
kpi_analytics = None
cubos_olap = {}

def inicializar_datos():
    """
    Inicializa los datos del sistema OLAP
    """
    global dataframes, kpi_analytics, cubos_olap
    
    try:
        print("[*] Cargando datos del Data Warehouse...")
        dataframes = cargar_datos_dw()
        
        if not dataframes or dataframes.get('vista_proyectos_completa', pd.DataFrame()).empty:
            raise ValueError("No se pudieron cargar los datos del DW")
        
        print("[*] Inicializando analytics de KPIs...")
        kpi_analytics = KPIAnalytics(dataframes)
        
        print("[*] Creando cubos OLAP...")
        df_principal = dataframes['vista_proyectos_completa']
        
        cubos_olap = {
            'principales': cubo_kpis_principales(df_principal),
            'temporal': cubo_kpis_temporal(df_principal),
            'categorias': cubo_kpis_por_categoria(df_principal)
        }
        
        print("[OK] Datos inicializados correctamente")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error inicializando datos: {e}")
        return False

@app.route('/')
def dashboard():
    """
    Página principal del dashboard
    """
    return render_template('dashboard.html')

@app.route('/api/kpis')
def api_kpis():
    """
    API para obtener todos los KPIs calculados
    """
    try:
        if kpi_analytics is None:
            return jsonify({'error': 'Sistema no inicializado'}), 500
        
        kpis = kpi_analytics.get_kpis()
        return jsonify({
            'success': True,
            'kpis': kpis,
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
        if kpi_analytics is None:
            return jsonify({'error': 'Sistema no inicializado'}), 500
        
        kpis_cliente = kpi_analytics.get_kpis_por_cliente()
        
        # Convertir a formato JSON serializable
        data = []
        for cliente, row in kpis_cliente.iterrows():
            data.append({
                'cliente': int(cliente),
                'cumplimiento_presupuesto': round(row['cumplimiento_presupuesto'], 2),
                'desviacion_presupuestal': round(row['desviacion_presupuestal'], 2),
                'proyectos_a_tiempo': round(row['proyectos_a_tiempo'] * 100, 2),
                'total_proyectos': int(row['total_proyectos']),
                'presupuesto_total': round(row['presupuesto_total'], 2)
            })
        
        return jsonify({
            'success': True,
            'data': data
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tendencias')
def api_tendencias():
    """
    API para obtener tendencias temporales de KPIs
    """
    try:
        if kpi_analytics is None:
            return jsonify({'error': 'Sistema no inicializado'}), 500
        
        tendencias = kpi_analytics.get_tendencias_temporales()
        
        # Convertir DataFrames a formato JSON
        data = {}
        for tipo, df in tendencias.items():
            if not df.empty:
                data[tipo] = df.to_dict('index')
        
        return jsonify({
            'success': True,
            'tendencias': data
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/alertas')
def api_alertas():
    """
    API para obtener alertas de KPIs fuera de umbral
    """
    try:
        if kpi_analytics is None:
            return jsonify({'error': 'Sistema no inicializado'}), 500
        
        alertas = kpi_analytics.get_alertas_kpis()
        
        return jsonify({
            'success': True,
            'alertas': alertas,
            'total_alertas': len(alertas)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cubo/<tipo>')
def api_cubo(tipo):
    """
    API para obtener datos de cubos OLAP específicos
    """
    try:
        if tipo not in cubos_olap:
            return jsonify({'error': f'Cubo {tipo} no encontrado'}), 404
        
        cubo = cubos_olap[tipo]
        if cubo.empty:
            return jsonify({'error': f'Cubo {tipo} está vacío'}), 404
        
        # Convertir MultiIndex a formato serializable
        data = []
        for idx, row in cubo.iterrows():
            item = {'index': idx if isinstance(idx, (str, int, float)) else list(idx)}
            item.update(row.to_dict())
            data.append(item)
        
        return jsonify({
            'success': True,
            'cubo': tipo,
            'data': data,
            'columns': list(cubo.columns),
            'shape': list(cubo.shape)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def api_stats():
    """
    API para obtener estadísticas generales del sistema
    """
    try:
        if not dataframes:
            return jsonify({'error': 'Sistema no inicializado'}), 500
        
        stats = {}
        for nombre, df in dataframes.items():
            if not df.empty:
                stats[nombre] = {
                    'registros': len(df),
                    'columnas': len(df.columns),
                    'memoria_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                }
        
        return jsonify({
            'success': True,
            'stats': stats,
            'total_tablas': len(stats)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 DASHBOARD KPIs OLAP - Sistema de Proyectos")
    print("=" * 60)
    
    # Inicializar datos
    if inicializar_datos():
        print(f"✅ Servidor iniciando en http://localhost:5000")
        print(f"📊 KPIs disponibles: {len(kpi_analytics.get_kpis()) if kpi_analytics else 0}")
        print(f"🔍 Cubos OLAP: {len(cubos_olap)}")
        print("=" * 60)
        
        app.run(debug=True, host='0.0.0.0', port=5000)
    else:
        print("❌ Error inicializando el sistema. Verifique la conexión al DW.")
        sys.exit(1)