"""
Orquesta el proceso completo de Extract, Transform, Load
"""
import logging
from typing import Dict
import pandas as pd

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Imports de módulos ETL
from extract.extract_gestion import extract_all, reset_incremental_control, get_last_extraction_info

# Imports de transformaciones individuales - Dimensiones
from transform.transform_dim.dim_clientes import transform as transform_dim_clientes
from transform.transform_dim.dim_empleados import transform as transform_dim_empleados
from transform.transform_dim.dim_proyectos import transform as transform_dim_proyectos
from transform.transform_dim.dim_tiempo import transform as transform_dim_tiempo
from transform.transform_dim.dim_hitos import transform as transform_dim_hitos
from transform.transform_dim.dim_tareas import transform as transform_dim_tareas
from transform.transform_dim.dim_pruebas import transform as transform_dim_pruebas
from transform.transform_dim.dim_gastos import transform as transform_dim_gastos

# Imports de transformaciones - Hechos
from transform.transform_fact.hechos_asignaciones import transform as transform_hechos_asignaciones
from transform.transform_fact.hechos_proyectos import transform as transform_hechos_proyectos

# Import de carga
from load.load_to_dw import load_all_to_dw

# from load.load_to_dw import load_all  # Comentado hasta implementar

def run_transformations(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    transformed_data = {}
    
    logger.info("=== INICIANDO TRANSFORMACIONES ===")
    
    # Transformar dimensiones
    logger.info("--- Transformando Dimensiones ---")
    
    try:
        # Dimensiones independientes primero
        transformed_data['dim_clientes'] = transform_dim_clientes(raw_data)
        transformed_data['dim_empleados'] = transform_dim_empleados(raw_data)
        transformed_data['dim_proyectos'] = transform_dim_proyectos(raw_data)
        transformed_data['dim_tiempo'] = transform_dim_tiempo(raw_data)
        transformed_data['dim_gastos'] = transform_dim_gastos(raw_data)
        
        # Dimensiones dependientes que necesitan otras dimensiones ya transformadas
        # dim_hitos necesita dim_proyectos
        combined_data_for_deps = {**raw_data, **transformed_data}
        transformed_data['dim_hitos'] = transform_dim_hitos(combined_data_for_deps)
        
        # dim_tareas y dim_pruebas necesitan dim_hitos (que ya está disponible)
        combined_data_for_deps2 = {**raw_data, **transformed_data}
        transformed_data['dim_tareas'] = transform_dim_tareas(combined_data_for_deps2)
        transformed_data['dim_pruebas'] = transform_dim_pruebas(combined_data_for_deps2)
        
        # Transformar hechos (necesitan tanto datos crudos como dimensiones)
        logger.info("--- Transformando Hechos ---")
        # Combinar datos crudos con dimensiones transformadas para los hechos
        combined_data = {**raw_data, **transformed_data}
        transformed_data['hechos_asignaciones'] = transform_hechos_asignaciones(combined_data)
        transformed_data['hechos_proyectos'] = transform_hechos_proyectos(combined_data)
        
        # Resumen de transformación
        logger.info("--- Resumen de Transformaciones ---")
        for table_name, df in transformed_data.items():
            logger.info(f"{table_name}: {len(df)} registros transformados")
            
    except Exception as e:
        logger.error(f"Error en transformaciones: {str(e)}")
        raise
    
    logger.info("=== TRANSFORMACIONES COMPLETADAS ===")
    return transformed_data

def run_etl_complete(incremental: bool = True, include_load: bool = True):
    """
    Ejecuta el proceso ETL completo (Extract, Transform, Load)
    
    Args:
        incremental: Si True, ejecuta extracción incremental
        include_load: Si True, incluye la fase de carga al DW
    """
    mode_msg = "INCREMENTAL" if incremental else "COMPLETA"
    phases_msg = "ETL COMPLETO" if include_load else "ET (Extract + Transform)"
    
    logger.info(f" INICIANDO {phases_msg} - MODO {mode_msg}")
    
    try:
        # Mostrar info de última extracción
        if incremental:
            last_date = get_last_extraction_info()
            logger.info(f" Última extracción: {last_date}")
        
        # 1. EXTRACCIÓN
        logger.info(f" FASE 1: EXTRACCIÓN {mode_msg}")
        raw_data = extract_all(incremental=incremental)
        
        if not raw_data:
            logger.warning("No se extrajeron datos. Finalizando proceso.")
            return None
        
        # 2. TRANSFORMACIÓN
        logger.info(" FASE 2: TRANSFORMACIÓN")
        transformed_data = run_transformations(raw_data)
        
        # 3. CARGA (opcional)
        if include_load:
            logger.info(" FASE 3: CARGA AL DATA WAREHOUSE")
            load_results = load_all_to_dw(transformed_data)
            logger.info(f" ETL COMPLETO (Extract + Transform + Load) completado exitosamente")
            return transformed_data, load_results
        else:
            logger.info(f" ET {mode_msg} (Extract + Transform) completado exitosamente")
            return transformed_data
        
    except Exception as e:
        logger.error(f" Error en {phases_msg} {mode_msg}: {str(e)}")
        raise

def run_extract_transform(incremental: bool = True):
    """Solo ejecuta Extract + Transform (sin Load)"""
    return run_etl_complete(incremental=incremental, include_load=False)

def run_etl():
    """
    Ejecuta el proceso ETL completo
    """
    logger.info(" INICIANDO ETL COMPLETO")
    
    try:
        # Extraer y transformar
        transformed_data = run_extract_transform()
        
        if transformed_data is None:
            return
        
        # 3. Carga (por implementar)
        logger.info(" FASE 3: CARGA")
        logger.warning("Módulo de carga aún no implementado")
        # load_all(transformed_data)
        
        logger.info(" ETL completo finalizado")
        
    except Exception as e:
        logger.error(f" Error en ETL completo: {str(e)}")
        raise

def test_etl(include_load: bool = False):
    """
    Función de prueba del ETL
    
    Args:
        include_load: Si True, ejecuta ETL completo con carga al DW
    """
    test_type = "ETL COMPLETO (con carga)" if include_load else "ETL (solo Extract + Transform)"
    print(f" EJECUTANDO PRUEBA DE {test_type}")
    
    try:
        if include_load:
            result = run_etl_complete(include_load=True)
            transformed_data, load_results = result if result else (None, None)
        else:
            transformed_data = run_extract_transform()
            load_results = None
        
        if transformed_data:
            print("\n RESULTADOS DE LA PRUEBA:")
            for table_name, df in transformed_data.items():
                print(f"\n--- {table_name.upper()} ---")
                print(f"Registros: {len(df)}")
                if len(df) > 0:
                    print("Primeras 3 filas:")
                    print(df.head(3))
                else:
                    print("No hay datos")
            
            if load_results:
                print(f"\n RESULTADOS DE CARGA AL DW:")
                for table_name, count in load_results.items():
                    status = "ok" if count > 0 else "warning"
                    print(f"{status} {table_name}: {count:,} registros cargados")
        
        print(f"\n Prueba completada exitosamente")
        return transformed_data, load_results if include_load else transformed_data
        
    except Exception as e:
        print(f"\n❌ Error en prueba: {str(e)}")
        return None

def run_full_load(include_load: bool = False):
    """
    Ejecutar carga completa (no incremental)
    
    Args:
        include_load: Si True, incluye carga al DW
    """
    logger.info("FORZANDO CARGA COMPLETA")
    return run_etl_complete(incremental=False, include_load=include_load)

def reset_and_run(include_load: bool = False):
    """
    Resetear control incremental y ejecutar carga completa
    
    Args:
        include_load: Si True, incluye carga al DW
    """
    logger.info(" RESETEANDO CONTROL INCREMENTAL")
    reset_incremental_control()
    return run_full_load(include_load=include_load)

def show_incremental_status():
    """
    Mostrar estado del control incremental
    """
    last_date = get_last_extraction_info()
    print(f" Última extracción: {last_date}")
    print(f" Próxima extracción será: INCREMENTAL (solo cambios desde {last_date})")
    print(" Para carga completa usar: reset_and_run() o run_full_load()")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            print("Ejecutando carga completa (Extract + Transform)...")
            run_full_load(include_load=False)
        elif sys.argv[1] == "--full-load":
            print("Ejecutando ETL COMPLETO con carga al DW...")
            run_full_load(include_load=True)
        elif sys.argv[1] == "--reset":
            print("Reseteando control y ejecutando carga completa...")
            reset_and_run(include_load=False)
        elif sys.argv[1] == "--reset-load":
            print("Reseteando control y ejecutando ETL COMPLETO con carga al DW...")
            reset_and_run(include_load=True)
        elif sys.argv[1] == "--test-load":
            print("Ejecutando prueba ETL COMPLETO con carga al DW...")
            test_etl(include_load=True)
        elif sys.argv[1] == "--status":
            show_incremental_status()
        else:
            print("Opciones disponibles:")
            print("  --full        : Carga completa (solo Extract + Transform)")
            print("  --full-load   : ETL completo con carga al DW")
            print("  --reset       : Reset + carga completa")
            print("  --reset-load  : Reset + ETL completo con carga al DW")
            print("  --test-load   : Prueba ETL completo con carga")
            print("  --status      : Mostrar estado incremental")
    else:
        # Ejecución normal (incremental, solo Extract + Transform)
        test_etl(include_load=False)
