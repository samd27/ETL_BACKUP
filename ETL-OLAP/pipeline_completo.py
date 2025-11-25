#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Completo: ETL + OLAP
Ejecuta el proceso completo de Extract-Transform-Load seguido del análisis OLAP

Este script orquesta todo el proceso:
1. Ejecuta el ETL completo
2. Verifica la carga exitosa del Data Warehouse
3. Ejecuta análisis OLAP multidimensional
4. Genera reportes y visualizaciones
"""

import sys
import os
import subprocess
import time
from datetime import datetime

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def ejecutar_comando(comando, descripcion):
    """
    Ejecuta un comando del sistema y maneja errores
    """
    print(f"\n🔄 {descripcion}...")
    print(f"Comando: {comando}")
    print("-" * 50)
    
    try:
        # Ejecutar comando
        resultado = subprocess.run(
            comando, 
            shell=True, 
            capture_output=True, 
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        # Mostrar salida
        if resultado.stdout:
            print(resultado.stdout)
        
        if resultado.stderr and resultado.returncode != 0:
            print(f"❌ Error: {resultado.stderr}")
            return False
        
        if resultado.returncode == 0:
            print(f"✅ {descripcion} completado exitosamente")
            return True
        else:
            print(f"❌ {descripcion} falló con código: {resultado.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando {descripcion}: {e}")
        return False

def verificar_requisitos():
    """
    Verifica que todos los requisitos estén disponibles
    """
    print("🔍 VERIFICANDO REQUISITOS...")
    print("-" * 40)
    
    requisitos = {
        'main_etl.py': 'Script principal ETL',
        'olap/analisis_olap.py': 'Script análisis OLAP',
        '.env': 'Archivo de configuración'
    }
    
    todos_ok = True
    for archivo, descripcion in requisitos.items():
        if os.path.exists(archivo):
            print(f"✅ {descripcion}: {archivo}")
        else:
            print(f"❌ {descripcion}: {archivo} - NO ENCONTRADO")
            todos_ok = False
    
    return todos_ok

def main():
    """
    Función principal que ejecuta todo el pipeline
    """
    inicio_total = time.time()
    
    print("[+] PIPELINE COMPLETO: ETL + OLAP")
    print("=" * 80)
    print(f"📅 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Verificar requisitos
    if not verificar_requisitos():
        print("❌ Faltan requisitos necesarios. Abortando...")
        return False
    
    # 2. Ejecutar ETL completo
    print("\n📥 FASE 1: EXTRACT-TRANSFORM-LOAD")
    print("=" * 50)
    
    etl_exitoso = ejecutar_comando(
        "python main_etl.py --test-load",
        "ETL Completo con carga al Data Warehouse"
    )
    
    if not etl_exitoso:
        print("❌ ETL falló. No se puede continuar con OLAP.")
        return False
    
    # Pausa breve para asegurar que la BD esté lista
    print("⏳ Esperando 3 segundos para asegurar consistencia de BD...")
    time.sleep(3)
    
    # 3. Ejecutar análisis OLAP
    print("\n📊 FASE 2: ANÁLISIS OLAP")
    print("=" * 50)
    
    olap_exitoso = ejecutar_comando(
        "python olap/analisis_olap.py",
        "Análisis OLAP multidimensional"
    )
    
    if not olap_exitoso:
        print("⚠️ OLAP falló, pero ETL fue exitoso")
        return False
    
    # 4. Resumen final
    tiempo_total = time.time() - inicio_total
    print("\n🎉 PIPELINE COMPLETADO EXITOSAMENTE")
    print("=" * 80)
    print(f"⏱️ Tiempo total: {tiempo_total:.2f} segundos")
    print(f"📅 Finalización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n📊 ARCHIVOS GENERADOS:")
    archivos_esperados = [
        'analisis_olap_proyectos.xlsx',
        'dataset_olap_proyectos.csv', 
        'reporte_ejecutivo.txt'
    ]
    
    for archivo in archivos_esperados:
        if os.path.exists(archivo):
            size = os.path.getsize(archivo)
            print(f"✅ {archivo} ({size:,} bytes)")
        else:
            print(f"⚠️ {archivo} - No generado")
    
    print("\n💡 PRÓXIMOS PASOS:")
    print("1. Revisar 'analisis_olap_proyectos.xlsx' para cubos OLAP")
    print("2. Abrir 'dataset_olap_proyectos.csv' para análisis adicional")
    print("3. Leer 'reporte_ejecutivo.txt' para insights automáticos")
    print("4. Ejecutar 'python olap/analisis_olap.py --interactivo' para exploración")
    
    return True

def ejecutar_solo_olap():
    """
    Ejecuta solo el análisis OLAP (asume que ETL ya fue ejecutado)
    """
    print("📊 EJECUTANDO SOLO ANÁLISIS OLAP")
    print("=" * 50)
    print("⚠️ Asumiendo que ETL ya fue ejecutado exitosamente")
    
    return ejecutar_comando(
        "python olap/analisis_olap.py",
        "Análisis OLAP multidimensional"
    )

def mostrar_ayuda():
    """
    Muestra la ayuda del script
    """
    print("[+] PIPELINE ETL + OLAP - Sistema de Análisis de Proyectos")
    print("=" * 70)
    print("\nUSO:")
    print("  python pipeline_completo.py [OPCIÓN]")
    print("\nOPCIONES:")
    print("  (sin argumentos)  Ejecuta pipeline completo (ETL + OLAP)")
    print("  --solo-olap       Ejecuta solo análisis OLAP")
    print("  --solo-etl        Ejecuta solo ETL")
    print("  --interactivo     Modo interactivo OLAP")
    print("  --help            Muestra esta ayuda")
    print("\nEJEMPLOS:")
    print("  python pipeline_completo.py                    # Pipeline completo")
    print("  python pipeline_completo.py --solo-olap        # Solo OLAP")
    print("  python pipeline_completo.py --interactivo      # Exploración interactiva")
    print("\nARCHIVOS GENERADOS:")
    print("  - analisis_olap_proyectos.xlsx  (Cubos OLAP)")
    print("  - dataset_olap_proyectos.csv    (Dataset completo)")
    print("  - reporte_ejecutivo.txt         (Reporte e insights)")

if __name__ == "__main__":
    # Procesar argumentos de línea de comandos
    if len(sys.argv) > 1:
        argumento = sys.argv[1].lower()
        
        if argumento == '--help' or argumento == '-h':
            mostrar_ayuda()
        elif argumento == '--solo-olap':
            ejecutar_solo_olap()
        elif argumento == '--solo-etl':
            ejecutar_comando(
                "python main_etl.py --test-load",
                "ETL Completo con carga al Data Warehouse"
            )
        elif argumento == '--interactivo':
            ejecutar_comando(
                "python olap/analisis_olap.py --interactivo",
                "Análisis OLAP interactivo"
            )
        else:
            print(f"❌ Argumento desconocido: {argumento}")
            print("💡 Usa --help para ver opciones disponibles")
    else:
        # Ejecutar pipeline completo
        main()