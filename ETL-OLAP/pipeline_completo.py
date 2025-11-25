#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline Completo: ETL + OLAP + Dashboard
Ejecuta el proceso completo de Extract-Transform-Load seguido del análisis OLAP y Dashboard

Este script orquesta todo el proceso:
1. Ejecuta el ETL completo
2. Verifica la carga exitosa del Data Warehouse
3. Ejecuta análisis OLAP multidimensional
4. Genera reportes y visualizaciones
5. Inicia dashboard web con KPIs
"""

import sys
import os
import subprocess
import time
from datetime import datetime
import threading

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def ejecutar_comando(comando, descripcion):
    """
    Ejecuta un comando del sistema y maneja errores
    """
    print(f"\n[INFO] {descripcion}...")
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
            print(f"[ERROR] Error: {resultado.stderr}")
            return False
        
        if resultado.returncode == 0:
            print(f"[OK] {descripcion} completado exitosamente")
            return True
        else:
            print(f"[ERROR] {descripcion} fallo con codigo: {resultado.returncode}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Error ejecutando {descripcion}: {e}")
        return False

def verificar_requisitos():
    """
    Verifica que todos los requisitos estén disponibles
    """
    print("[INFO] VERIFICANDO REQUISITOS...")
    print("-" * 40)
    
    requisitos = {
        'main_etl.py': 'Script principal ETL',
        'OLAP/generar_cubos_kpis.py': 'Generador de cubos OLAP',
        'dashboard_simple.py': 'Dashboard Web',
        '.env': 'Archivo de configuracion'
    }
    
    todos_ok = True
    for archivo, descripcion in requisitos.items():
        if os.path.exists(archivo):
            print(f"[OK] {descripcion}: {archivo}")
        else:
            print(f"[ERROR] {descripcion}: {archivo} - NO ENCONTRADO")
            todos_ok = False
    
    return todos_ok

def iniciar_dashboard_background():
    """
    Inicia el dashboard en segundo plano
    """
    try:
        print("[INFO] Iniciando dashboard web en puerto 8080...")
        subprocess.Popen(
            ["python", "dashboard_simple.py"],
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        time.sleep(2)
        print("[OK] Dashboard iniciado - http://localhost:8080")
    except Exception as e:
        print(f"[ERROR] Error al iniciar dashboard: {e}")

def main():
    """
    Función principal que ejecuta todo el pipeline
    """
    inicio_total = time.time()
    
    print("[+] PIPELINE COMPLETO: ETL + OLAP + DASHBOARD")
    print("=" * 80)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Verificar requisitos
    if not verificar_requisitos():
        print("[ERROR] Faltan requisitos necesarios. Abortando...")
        return False
    
    # 2. Ejecutar ETL completo
    print("\n[FASE 1] EXTRACT-TRANSFORM-LOAD")
    print("=" * 50)
    
    etl_exitoso = ejecutar_comando(
        "python main_etl.py --test-load",
        "ETL Completo con carga al Data Warehouse"
    )
    
    if not etl_exitoso:
        print("[ERROR] ETL fallo. No se puede continuar con OLAP.")
        return False
    
    # Pausa breve para asegurar que la BD esté lista
    print("[INFO] Esperando 3 segundos para asegurar consistencia de BD...")
    time.sleep(3)
    
    # 3. Ejecutar análisis OLAP
    print("\n[FASE 2] ANALISIS OLAP")
    print("=" * 50)
    
    olap_exitoso = ejecutar_comando(
        "python OLAP/generar_cubos_kpis.py",
        "Generacion de cubos OLAP para KPIs"
    )
    
    if not olap_exitoso:
        print("[WARNING] OLAP fallo, pero ETL fue exitoso")
        return False
    
    # 4. Iniciar dashboard
    print("\n[FASE 3] DASHBOARD WEB")
    print("=" * 50)
    iniciar_dashboard_background()
    
    # 5. Resumen final
    tiempo_total = time.time() - inicio_total
    print("\n[EXITO] PIPELINE COMPLETADO EXITOSAMENTE")
    print("=" * 80)
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    print(f"Finalizacion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n[CUBOS OLAP] CUBOS GENERADOS EN MEMORIA:")
    print("[OK] Cubos OLAP disponibles para calculo de KPIs")
    print("[OK] Sistema optimizado - No genera archivos innecesarios")
    
    print("\n[PROXIMOS PASOS]")
    print("1. Acceder al dashboard web en http://localhost:8080")
    print("2. Los KPIs se calculan automaticamente desde los cubos OLAP")
    print("3. Los cubos OLAP estan disponibles en memoria para el dashboard")
    
    return True

def ejecutar_solo_olap():
    """
    Ejecuta solo el análisis OLAP (asume que ETL ya fue ejecutado)
    """
    print("[INFO] EJECUTANDO SOLO ANALISIS OLAP")
    print("=" * 50)
    print("[WARNING] Asumiendo que ETL ya fue ejecutado exitosamente")
    
    return ejecutar_comando(
        "python OLAP/generar_cubos_kpis.py",
        "Generacion de cubos OLAP para KPIs"
    )

def ejecutar_solo_dashboard():
    """
    Ejecuta solo el dashboard (asume que ETL y OLAP ya fueron ejecutados)
    """
    print("[INFO] EJECUTANDO SOLO DASHBOARD")
    print("=" * 50)
    print("[WARNING] Asumiendo que ETL y OLAP ya fueron ejecutados")
    
    iniciar_dashboard_background()
    print("[INFO] Dashboard disponible en http://localhost:8080")
    print("[INFO] Presiona Ctrl+C para detener")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Dashboard detenido")

def mostrar_ayuda():
    """
    Muestra la ayuda del script
    """
    print("[+] PIPELINE ETL + OLAP + DASHBOARD - Sistema de Analisis de Proyectos")
    print("=" * 80)
    print("\nUSO:")
    print("  python pipeline_completo.py [OPCION]")
    print("\nOPCIONES:")
    print("  (sin argumentos)  Ejecuta pipeline completo (ETL + OLAP + Dashboard)")
    print("  --solo-olap       Ejecuta solo analisis OLAP")
    print("  --solo-etl        Ejecuta solo ETL")
    print("  --solo-dashboard  Ejecuta solo Dashboard Web")
    print("  --interactivo     Modo interactivo OLAP")
    print("  --help            Muestra esta ayuda")
    print("\nEJEMPLOS:")
    print("  python pipeline_completo.py                    # Pipeline completo")
    print("  python pipeline_completo.py --solo-olap        # Solo OLAP")
    print("  python pipeline_completo.py --solo-dashboard   # Solo Dashboard")
    print("  python pipeline_completo.py --interactivo      # Exploracion interactiva")
    print("\nCUBOS OLAP GENERADOS:")
    print("  - Cubos principales para calculo de KPIs")
    print("  - Cubos temporales para analisis de tendencias")
    print("  - Cubos por categorias para analisis detallado")
    print("\nSERVICIOS WEB:")
    print("  - Dashboard KPIs: http://localhost:8080")

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
        elif argumento == '--solo-dashboard':
            ejecutar_solo_dashboard()
        elif argumento == '--interactivo':
            ejecutar_comando(
                "python OLAP/generar_cubos_kpis.py",
                "Generacion de cubos OLAP para KPIs"
            )
        else:
            print(f"[ERROR] Argumento desconocido: {argumento}")
            print("[INFO] Usa --help para ver opciones disponibles")
    else:
        # Ejecutar pipeline completo
        main()