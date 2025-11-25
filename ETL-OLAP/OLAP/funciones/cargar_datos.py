import pandas as pd
import mysql.connector
from typing import Dict, Optional
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.db_config import get_dw_connection

def cargar_datos_dw() -> Dict[str, pd.DataFrame]:
    """
    Carga todos los datos del Data Warehouse para análisis OLAP.
    Retorna un diccionario con DataFrames de todas las tablas.
    """
    print("[*] Cargando datos del Data Warehouse...")
    
    connection = get_dw_connection()
    
    try:
        # Definir queries para cada tabla con joins necesarios
        queries = {
            # Dimensiones base
            'dim_clientes': "SELECT * FROM dim_clientes",
            'dim_empleados': "SELECT * FROM dim_empleados", 
            'dim_proyectos': "SELECT * FROM dim_proyectos",
            'dim_tiempo': "SELECT * FROM dim_tiempo",
            'dim_gastos': "SELECT * FROM dim_gastos",
            'dim_hitos': "SELECT * FROM dim_hitos",
            'dim_tareas': "SELECT * FROM dim_tareas",
            'dim_pruebas': "SELECT * FROM dim_pruebas",
            
            # Hechos
            'hechos_asignaciones': "SELECT * FROM hechos_asignaciones",
            'hechos_proyectos': "SELECT * FROM hechos_proyectos",
            
            # Vistas analíticas (con joins)
            'vista_proyectos_completa': """
                SELECT 
                    hp.*,
                    dp.CodigoProyecto, dp.Version, dp.Cancelado, dp.TotalErrores, dp.NumTrabajadores,
                    dc.CodigoClienteReal,
                    dt_inicio.Anio as AnioInicio, dt_inicio.Mes as MesInicio,
                    dt_fin.Anio as AnioFin, dt_fin.Mes as MesFin,
                    dg.TipoGasto, dg.Categoria as CategoriaGasto, dg.Monto as MontoGasto
                FROM hechos_proyectos hp
                LEFT JOIN dim_proyectos dp ON hp.ID_Proyecto = dp.ID_Proyecto
                LEFT JOIN dim_clientes dc ON dp.ID_Cliente = dc.ID_Cliente
                LEFT JOIN dim_tiempo dt_inicio ON hp.ID_FechaInicio = dt_inicio.ID_Tiempo
                LEFT JOIN dim_tiempo dt_fin ON hp.ID_FechaFin = dt_fin.ID_Tiempo
                LEFT JOIN dim_gastos dg ON hp.ID_Gasto = dg.ID_Finanza
            """,
            
            'vista_asignaciones_completa': """
                SELECT 
                    ha.*,
                    de.CodigoEmpleado, de.Rol, de.Seniority,
                    dp.CodigoProyecto, dp.Version, dp.Cancelado,
                    dc.CodigoClienteReal,
                    dt.Anio, dt.Mes, dt.Dia
                FROM hechos_asignaciones ha
                LEFT JOIN dim_empleados de ON ha.ID_Empleado = de.ID_Empleado
                LEFT JOIN dim_proyectos dp ON ha.ID_Proyecto = dp.ID_Proyecto
                LEFT JOIN dim_clientes dc ON dp.ID_Cliente = dc.ID_Cliente
                LEFT JOIN dim_tiempo dt ON ha.ID_FechaAsignacion = dt.ID_Tiempo
            """,
            
            'vista_hitos_tareas': """
                SELECT 
                    dh.ID_Hito, dh.CodigoHito, dh.ID_proyectos,
                    dh.RetrasoInicioDias as HitoRetrasoInicio, dh.RetrasoFinDias as HitoRetrasoFin,
                    dp.CodigoProyecto, dp.Version, dp.Cancelado,
                    dc.CodigoClienteReal,
                    dt.ID_Tarea, dt.CodigoTarea, dt.SeRetraso as TareaSeRetraso,
                    COUNT(dpr.ID_Prueba) as TotalPruebas,
                    SUM(dpr.PruebaExitosa) as PruebasExitosas
                FROM dim_hitos dh
                LEFT JOIN dim_proyectos dp ON dh.ID_proyectos = dp.ID_Proyecto
                LEFT JOIN dim_clientes dc ON dp.ID_Cliente = dc.ID_Cliente
                LEFT JOIN dim_tareas dt ON dh.ID_Hito = dt.ID_Hito
                LEFT JOIN dim_pruebas dpr ON dh.ID_Hito = dpr.ID_Hito
                GROUP BY dh.ID_Hito, dh.CodigoHito, dh.ID_proyectos, 
                         dh.RetrasoInicioDias, dh.RetrasoFinDias,
                         dp.CodigoProyecto, dp.Version, dp.Cancelado,
                         dc.CodigoClienteReal, dt.ID_Tarea, dt.CodigoTarea, dt.SeRetraso
            """
        }
        
        # Ejecutar queries y cargar datos
        dataframes = {}
        for tabla, query in queries.items():
            try:
                df = pd.read_sql(query, connection)
                dataframes[tabla] = df
                print(f"[OK] {tabla}: {len(df)} registros cargados")
            except Exception as e:
                print(f"⚠️ Error cargando {tabla}: {e}")
                dataframes[tabla] = pd.DataFrame()
        
        print(f"\n[*] Total de tablas cargadas: {len([df for df in dataframes.values() if not df.empty])}")
        return dataframes
        
    except Exception as e:
        print(f"❌ Error general cargando datos: {e}")
        return {}
    finally:
        if connection.is_connected():
            connection.close()
            print("[*] Conexión al DW cerrada")

def preparar_dataset_olap(dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Prepara el dataset principal para análisis OLAP combinando todas las dimensiones.
    Retorna un DataFrame desnormalizado optimizado para operaciones OLAP.
    """
    print("\n[*] Preparando dataset OLAP desnormalizado...")
    
    # Usar la vista completa como base
    vista_proyectos = dataframes.get('vista_proyectos_completa', pd.DataFrame())
    
    if vista_proyectos.empty:
        print("❌ No hay datos de vista_proyectos_completa")
        return pd.DataFrame()
    
    # Agregar categorías analíticas
    df_olap = vista_proyectos.copy()
    
    # Categorías de análisis
    df_olap['Estado'] = df_olap['Cancelado'].map({0: 'Cerrado', 1: 'Cancelado'})
    
    # Categorías de presupuesto
    df_olap['CategoriaPresupuesto'] = pd.cut(
        df_olap['Presupuesto'], 
        bins=[0, 50000, 100000, 200000, float('inf')],
        labels=['Pequeño', 'Mediano', 'Grande', 'Mega']
    )
    
    # Categorías de desviación presupuestal
    df_olap['TipoDesviacion'] = df_olap['DesviacionPresupuestal'].apply(
        lambda x: 'Sobre Presupuesto' if x < 0 else 'Bajo Presupuesto' if x > 0 else 'En Presupuesto'
    )
    
    # Categorías de productividad
    df_olap['CategoriaProductividad'] = pd.cut(
        df_olap['ProductividadPromedio'],
        bins=[0, 200, 400, 600, float('inf')],
        labels=['Baja', 'Media', 'Alta', 'Muy Alta']
    )
    
    # Categorías de calidad (basado en tasa de éxito en pruebas)
    df_olap['CategoriaCalidad'] = pd.cut(
        df_olap['TasaDeExitoEnPruebas'],
        bins=[0, 0.7, 0.85, 0.95, 1.0],
        labels=['Baja', 'Media', 'Alta', 'Excelente']
    )
    
    # Categorías temporales
    df_olap['PeriodoInicio'] = df_olap['AnioInicio'].astype(str) + '-Q' + \
                               ((df_olap['MesInicio'] - 1) // 3 + 1).astype(str)
    
    print(f"[OK] Dataset OLAP preparado: {len(df_olap)} registros con {len(df_olap.columns)} dimensiones")
    
    return df_olap

def mostrar_resumen_datos(dataframes: Dict[str, pd.DataFrame]):
    """Muestra un resumen de los datos cargados"""
    print("\n[*] RESUMEN DE DATOS CARGADOS:")
    print("=" * 50)
    
    for tabla, df in dataframes.items():
        if not df.empty:
            print(f"[*] {tabla.upper()}: {len(df)} registros, {len(df.columns)} columnas")
        else:
            print(f"[!] {tabla.upper()}: Sin datos")
    
    # Mostrar algunas estadísticas clave si tenemos datos de proyectos
    vista_proyectos = dataframes.get('vista_proyectos_completa', pd.DataFrame())
    if not vista_proyectos.empty:
        print(f"\n[*] MÉTRICAS CLAVE:")
        print(f"[+] Presupuesto total: ${vista_proyectos['Presupuesto'].sum():,.2f}")
        print(f"[+] Costo real total: ${vista_proyectos['CosteReal'].sum():,.2f}")
        print(f"[*] Desviación promedio: ${vista_proyectos['DesviacionPresupuestal'].mean():,.2f}")
        print(f"[+] Productividad promedio: {vista_proyectos['ProductividadPromedio'].mean():.2f} hrs/hito")
        print(f"[OK] Tasa de éxito promedio: {vista_proyectos['TasaDeExitoEnPruebas'].mean():.2%}")