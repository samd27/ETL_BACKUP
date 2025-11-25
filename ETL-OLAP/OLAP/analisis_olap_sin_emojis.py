#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema OLAP para Análisis de Proyectos
Integrado con el Data Warehouse del sistema ETL

Este módulo se ejecuta después del ETL para realizar análisis multidimensional
de los datos de proyectos cargados en el Data Warehouse.
"""

import pandas as pd
import sys
import os
from typing import Dict, Any

# Configurar pandas para mejor visualización
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

# Imports del sistema OLAP
from funciones.cargar_datos import cargar_datos_dw, preparar_dataset_olap, mostrar_resumen_datos
from funciones.crear_cubos import (
    cubo_base_proyectos, cubo_productividad_calidad, cubo_financiero,
    cubo_temporal_estado, cubo_multimedidas, cubo_kpis_ejecutivos,
    crear_cubo_personalizado
)
from funciones.operaciones_olap import (
    slice_por_cliente, slice_por_estado, slice_por_anio,
    dice_subset, rollup_por_cliente, rollup_por_estado, rollup_por_anio,
    rollup_jerarquico, drilldown_cliente_detallado, drilldown_temporal,
    pivot_cliente_estado, pivot_anio_calidad, pivot_presupuesto_productividad
)
from funciones.reportes import (
    generar_reporte_ejecutivo, imprimir_reporte_ejecutivo,
    exportar_cubos_excel, generar_insights_automaticos
)

def ejecutar_demo_olap():
    """
    Ejecuta una demostración completa del sistema OLAP
    """
    print("[+] SISTEMA OLAP - ANÁLISIS MULTIDIMENSIONAL DE PROYECTOS")
    print("=" * 80)
    
    # 1. CARGAR DATOS DEL DATA WAREHOUSE
    print("\\n[1] PASO 1: CARGA DE DATOS")
    print("-" * 40)
    dataframes = cargar_datos_dw()
    
    if not dataframes or dataframes.get('vista_proyectos_completa', pd.DataFrame()).empty:
        print("[ERROR] No se pudieron cargar datos del Data Warehouse")
        print("[INFO] Asegúrate de que el ETL se haya ejecutado exitosamente")
        return
    
    mostrar_resumen_datos(dataframes)
    
    # 2. PREPARAR DATASET OLAP
    print("\\n[2] PASO 2: PREPARACIÓN DEL DATASET OLAP")
    print("-" * 40)
    df_olap = preparar_dataset_olap(dataframes)
    
    if df_olap.empty:
        print("[ERROR] No se pudo preparar el dataset OLAP")
        return
    
    print(f"[OK] Dataset OLAP listo: {len(df_olap)} proyectos, {len(df_olap.columns)} dimensiones")
    print(f"[INFO] Dimensiones disponibles: {list(df_olap.columns)}")
    
    # 3. CREAR CUBOS OLAP
    print("\\n[3] PASO 3: CREACIÓN DE CUBOS OLAP")
    print("-" * 40)
    
    cubos = {}
    
    try:
        # Cubo base
        print("[*] Creando cubo base (Cliente x Estado x Periodo)...")
        cubos['cubo_base'] = cubo_base_proyectos(df_olap)
        
        # Cubo financiero
        print("[*] Creando cubo financiero...")
        cubos['cubo_financiero'] = cubo_financiero(df_olap)
        
        # Cubo productividad-calidad
        print("[*] Creando cubo productividad-calidad...")
        cubos['cubo_productividad_calidad'] = cubo_productividad_calidad(df_olap)
        
        # Cubo temporal
        print("[*] Creando cubo temporal...")
        cubos['cubo_temporal'] = cubo_temporal_estado(df_olap)
        
        # Cubo KPIs ejecutivos
        print("[*] Creando cubo KPIs ejecutivos...")
        cubos['cubo_kpis'] = cubo_kpis_ejecutivos(df_olap)
        
        print(f"[OK] {len(cubos)} cubos creados exitosamente")
        
    except Exception as e:
        print(f"[ERROR] Error creando cubos: {e}")
        return
    
    # 4. DEMOSTRACIÓN DE OPERACIONES OLAP
    print("\\n[4] PASO 4: OPERACIONES OLAP")
    print("-" * 40)
    
    # Slice - Filtrar por un cliente específico
    if df_olap['CodigoClienteReal'].nunique() > 0:
        cliente_ejemplo = df_olap['CodigoClienteReal'].iloc[0]
        slice_cliente = slice_por_cliente(df_olap, cliente_ejemplo)
        print(f"[*] SLICE - Cliente {cliente_ejemplo}: {len(slice_cliente)} proyectos")
        if not slice_cliente.empty:
            print(slice_cliente[['CodigoProyecto', 'Estado', 'Presupuesto', 'CosteReal']].head(3))
    
    # Dice - Filtrar proyectos grandes cerrados de últimos años
    dice_ejemplo = dice_subset(
        df_olap,
        estados=['Cerrado'],
        categorias_presupuesto=['Grande', 'Mega'],
        anios=[2024, 2025] if 2024 in df_olap['AnioInicio'].values else None
    )
    print(f"\\n[*] DICE - Proyectos grandes cerrados: {len(dice_ejemplo)} proyectos")
    if not dice_ejemplo.empty:
        print(dice_ejemplo[['CodigoProyecto', 'CategoriaPresupuesto', 'Presupuesto']].head(3))
    
    # Roll-up - Agregación por cliente
    rollup_clientes = rollup_por_cliente(df_olap)
    print(f"\\n[*] ROLL-UP - Resumen por cliente:")
    print(rollup_clientes.head(5))
    
    # Drill-down - Detalle de un cliente
    if df_olap['CodigoClienteReal'].nunique() > 0:
        cliente_drill = df_olap['CodigoClienteReal'].iloc[0]
        drilldown_detalle = drilldown_cliente_detallado(df_olap, cliente_drill)
        print(f"\\n[*] DRILL-DOWN - Detalle cliente {cliente_drill}:")
        if not drilldown_detalle.empty:
            print(drilldown_detalle.head(3))
    
    # Pivot - Cliente vs Estado
    pivot_ejemplo = pivot_cliente_estado(df_olap)
    print(f"\\n[*] PIVOT - Cliente vs Estado:")
    print(pivot_ejemplo.head(5))
    
    # 5. GENERAR REPORTES
    print("\\n[5] PASO 5: GENERACIÓN DE REPORTES")
    print("-" * 40)
    
    # Reporte ejecutivo
    reporte = generar_reporte_ejecutivo(df_olap)
    imprimir_reporte_ejecutivo(reporte)
    
    # Insights automáticos
    insights = generar_insights_automaticos(df_olap)
    print(f"\\n[*] INSIGHTS AUTOMÁTICOS:")
    print("-" * 40)
    for i, insight in enumerate(insights, 1):
        print(f"{i}. {insight}")
    
    # 6. EXPORTAR RESULTADOS
    print(f"\\n[6] PASO 6: EXPORTACIÓN DE RESULTADOS")
    print("-" * 40)
    
    try:
        # Exportar cubos a Excel
        exportar_cubos_excel(cubos, 'analisis_olap_proyectos.xlsx')
        
        # Guardar dataset principal
        df_olap.to_csv('dataset_olap_proyectos.csv', index=False)
        print("[OK] Dataset OLAP guardado como 'dataset_olap_proyectos.csv'")
        
        # Guardar reporte ejecutivo
        with open('reporte_ejecutivo.txt', 'w', encoding='utf-8') as f:
            f.write("REPORTE EJECUTIVO - ANÁLISIS OLAP DE PROYECTOS\\n")
            f.write("=" * 60 + "\\n")
            f.write(f"Fecha: {reporte['fecha_generacion']}\\n")
            f.write(f"Total proyectos: {reporte['total_proyectos']}\\n")
            f.write(f"Clientes activos: {reporte['clientes_activos']}\\n\\n")
            
            f.write("INSIGHTS AUTOMÁTICOS:\\n")
            for insight in insights:
                f.write(f"- {insight}\\n")
        
        print("[OK] Reporte ejecutivo guardado como 'reporte_ejecutivo.txt'")
        
    except Exception as e:
        print(f"[WARNING] Error en exportación: {e}")
    
    print(f"\\n[SUCCESS] ANÁLISIS OLAP COMPLETADO EXITOSAMENTE")
    print("=" * 80)
    print("[INFO] Archivos generados:")
    print("   - analisis_olap_proyectos.xlsx (Cubos OLAP)")
    print("   - dataset_olap_proyectos.csv (Dataset completo)")
    print("   - reporte_ejecutivo.txt (Reporte e insights)")

def ejecutar_analisis_interactivo():
    """
    Modo interactivo para explorar los datos OLAP
    """
    print("[+] MODO INTERACTIVO - EXPLORACIÓN OLAP")
    print("=" * 50)
    
    # Cargar datos
    dataframes = cargar_datos_dw()
    df_olap = preparar_dataset_olap(dataframes)
    
    if df_olap.empty:
        print("[ERROR] No hay datos disponibles")
        return
    
    while True:
        print("\\n[*] OPCIONES DISPONIBLES:")
        print("1. Ver resumen general")
        print("2. Slice por cliente")
        print("3. Dice con filtros múltiples")
        print("4. Roll-up por dimensión")
        print("5. Pivot personalizado")
        print("6. Generar reporte")
        print("0. Salir")
        
        opcion = input("\\nSelecciona una opción (0-6): ").strip()
        
        if opcion == '0':
            print("[*] ¡Hasta luego!")
            break
        elif opcion == '1':
            print("\\n[INFO] RESUMEN GENERAL:")
            print(f"Total proyectos: {len(df_olap)}")
            print(f"Clientes únicos: {df_olap['CodigoClienteReal'].nunique()}")
            print(f"Estados: {df_olap['Estado'].unique()}")
            print(f"Rango temporal: {df_olap['AnioInicio'].min()} - {df_olap['AnioInicio'].max()}")
        elif opcion == '2':
            clientes = df_olap['CodigoClienteReal'].unique()
            print(f"\\nClientes disponibles: {sorted(clientes)}")
            try:
                cliente = int(input("Ingresa el código del cliente: "))
                resultado = slice_por_cliente(df_olap, cliente)
                print(f"\\n[INFO] Proyectos del cliente {cliente}: {len(resultado)}")
                if not resultado.empty:
                    print(resultado[['CodigoProyecto', 'Estado', 'Presupuesto', 'CosteReal']].head(10))
            except ValueError:
                print("[ERROR] Código de cliente inválido")
        elif opcion == '6':
            reporte = generar_reporte_ejecutivo(df_olap)
            imprimir_reporte_ejecutivo(reporte)
        else:
            print("[WARNING] Opción no implementada en modo interactivo")

if __name__ == "__main__":
    print("[+] SISTEMA OLAP - ANÁLISIS DE PROYECTOS")
    print("Integrado con Data Warehouse ETL")
    print("=" * 80)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--interactivo':
        ejecutar_analisis_interactivo()
    else:
        ejecutar_demo_olap()