"""
Módulo de extracción de datos del Sistema de Gestión de Proyectos (SGP)

REGLAS DE NEGOCIO:
1. SOLO se extraen datos de proyectos con Estado = 'Cerrado' OR 'Cancelado'
2. O de contratos con Estado = 'Cerrado' OR 'Cancelado'  
3. CARGA INCREMENTAL: Solo registros nuevos desde última extracción
4. Esta regla se aplica en cascada a todas las tablas relacionadas

"""

import mysql.connector
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Any
import sys
import os

# Agregar el directorio padre al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import DB_OLTP
from utils.helpers import get_connection
from utils.incremental_control import IncrementalControl

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SGPExtractor:
    
    def __init__(self, incremental: bool = True):
        self.connection = None
        self.extraction_timestamp = datetime.now()
        self.incremental = incremental
        self.control = IncrementalControl() if incremental else None
        
    def connect(self):
        try:
            self.connection = get_connection("OLTP")
            logger.info("Conexión establecida con SGP exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error conectando a SGP: {str(e)}")
            return False
    
    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Conexión cerrada exitosamente")
    
    def execute_query(self, query: str, table_name: str) -> pd.DataFrame:
        try:
            df = pd.read_sql(query, self.connection)
            mode = "INCREMENTAL" if self.incremental else "COMPLETA"
            logger.info(f"Extraídos {len(df)} registros de {table_name} [MODO: {mode}]")
            return df
        except Exception as e:
            logger.error(f"Error extrayendo datos de {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def get_incremental_filter(self) -> str:
        """
        Obtener filtro para carga incremental
        PROYECTO ESCOLAR: Siempre hacer carga completa para simplicidad
        """
        # Para proyecto escolar: siempre extraer todo (sin filtro incremental)
        return ""

    # ================= TABLAS =================
    
    def extract_clientes(self) -> pd.DataFrame:
        query = """
        SELECT DISTINCT
            cl.ID_Cliente,
            cl.NombreCliente,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM clientes cl
        INNER JOIN contratos c ON cl.ID_Cliente = c.ID_Cliente
        INNER JOIN proyectos p ON c.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY cl.ID_Cliente
        """
        return self.execute_query(query, "clientes")
    
    def extract_empleados(self) -> pd.DataFrame:
        query = """
        SELECT DISTINCT
            e.ID_Empleado,
            e.NombreCompleto,
            e.Rol,
            e.Seniority,
            e.CostoPorHora,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM empleados e
        INNER JOIN asignaciones a ON e.ID_Empleado = a.ID_Empleado
        INNER JOIN proyectos p ON a.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY e.ID_Empleado
        """
        return self.execute_query(query, "empleados")
    
    def extract_contratos(self) -> pd.DataFrame:
        query = """
        SELECT 
            c.ID_Contrato,
            c.ID_Cliente,
            c.ID_Proyecto,
            c.ValorTotalContrato,
            c.Estado,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM contratos c
        INNER JOIN proyectos p ON c.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY c.ID_Contrato
        """
        return self.execute_query(query, "contratos")
    
    def extract_proyectos(self) -> pd.DataFrame:
        incremental_filter = self.get_incremental_filter()
        
        query = f"""
        SELECT 
            p.ID_Proyecto,
            p.NombreProyecto,
            p.Version,
            p.FechaInicio,
            p.FechaFin,
            p.FechaInicioReal,
            p.FechaFinReal,
            p.Estado as EstadoProyecto,
            p.TipoProyecto,
            c.ID_Contrato,
            c.ID_Cliente,
            c.ValorTotalContrato,
            c.Estado as EstadoContrato,
            CASE 
                WHEN p.Estado IN ('Cerrado', 'Cancelado') THEN 'Por proyecto'
                ELSE 'No aplica'
            END as razon_inclusion,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM proyectos p
        LEFT JOIN contratos c ON c.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        {incremental_filter}
        ORDER BY p.ID_Proyecto
        """
        return self.execute_query(query, "proyectos")
    
    def extract_hitos(self) -> pd.DataFrame:
        query = """
        SELECT 
            h.ID_Hito,
            h.ID_Proyecto,
            h.Descripcion,
            h.Estado,
            h.FechaInicio,
            h.FechaFinPlanificada,
            h.FechaFinReal,
            DATEDIFF(IFNULL(h.FechaFinReal, CURDATE()), h.FechaFinPlanificada) as dias_retraso,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM hitos h
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY h.ID_Proyecto, h.ID_Hito
        """
        return self.execute_query(query, "hitos")
    
    def extract_tareas(self) -> pd.DataFrame:
        query = """
        SELECT 
            t.ID_Tarea,
            t.ID_Hito,
            h.ID_Proyecto,
            t.NombreTarea,
            t.Descripcion,
            t.Estado,
            t.FechaInicioPlanificada,
            t.FechaInicioReal,
            t.FechaFinPlanificada,
            t.FechaFinReal,
            DATEDIFF(IFNULL(t.FechaFinReal, CURDATE()), t.FechaFinPlanificada) as dias_retraso,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM tareas t
        INNER JOIN hitos h ON t.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY h.ID_Proyecto, t.ID_Hito, t.ID_Tarea
        """
        return self.execute_query(query, "tareas")
    
    def extract_asignaciones(self) -> pd.DataFrame:
        query = """
        SELECT 
            a.ID_Asignacion,
            a.ID_Proyecto,
            a.ID_Empleado,
            a.HorasPlanificadas,
            a.HorasReales,
            a.FechaAsignacion,
            e.CostoPorHora,
            (a.HorasReales * e.CostoPorHora) as costo_real_horas,
            (a.HorasPlanificadas * e.CostoPorHora) as costo_planificado_horas,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM asignaciones a
        INNER JOIN empleados e ON a.ID_Empleado = e.ID_Empleado
        INNER JOIN proyectos p ON a.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY a.ID_Proyecto, a.FechaAsignacion
        """
        return self.execute_query(query, "asignaciones")
    
    def extract_pruebas(self) -> pd.DataFrame:
        query = """
        SELECT 
            pr.ID_Prueba,
            pr.ID_Hito,
            h.ID_Proyecto,
            pr.TipoPrueba,
            pr.Fecha,
            pr.Exitosa,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM pruebas pr
        INNER JOIN hitos h ON pr.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY h.ID_Proyecto, pr.ID_Hito, pr.Fecha
        """
        return self.execute_query(query, "pruebas")
    
    def extract_errores(self) -> pd.DataFrame:
        query = """
        SELECT 
            e.ID_Error,
            e.ID_Tarea,
            t.ID_Hito,
            h.ID_Proyecto,
            e.TipoError,
            e.Descripcion,
            e.FaseDeteccion,
            e.FechaDeteccion,
            e.FechaCorreccion,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM errores e
        INNER JOIN tareas t ON e.ID_Tarea = t.ID_Tarea
        INNER JOIN hitos h ON t.ID_Hito = h.ID_Hito
        INNER JOIN proyectos p ON h.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY h.ID_Proyecto, e.FechaDeteccion
        """
        return self.execute_query(query, "errores")
    
    def extract_riesgos(self) -> pd.DataFrame:
        # La tabla riesgos no existe en el esquema real
        logger.info("La tabla 'riesgos' no existe en el esquema actual - retornando DataFrame vacío")
        return pd.DataFrame()
    
    def extract_gastos(self) -> pd.DataFrame:
        query = """
        SELECT 
            g.ID_Gasto,
            g.ID_Proyecto,
            g.TipoGasto,
            g.Categoria,
            g.Monto,
            g.Fecha,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM gastos g
        INNER JOIN proyectos p ON g.ID_Proyecto = p.ID_Proyecto
        WHERE p.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY g.ID_Proyecto, g.Fecha
        """
        return self.execute_query(query, "gastos")
    
    def extract_penalizaciones(self) -> pd.DataFrame:
        query = """
        SELECT 
            p.ID_Penalizacion,
            p.ID_Contrato,
            c.ID_Cliente,
            p.Monto,
            p.Motivo,
            p.Fecha,
            CURRENT_TIMESTAMP as fecha_extraccion
        FROM penalizaciones_contrato p
        INNER JOIN contratos c ON p.ID_Contrato = c.ID_Contrato
        INNER JOIN proyectos pr ON c.ID_Proyecto = pr.ID_Proyecto
        WHERE pr.Estado IN ('Cerrado', 'Cancelado')
        ORDER BY p.ID_Contrato, p.Fecha
        """
        return self.execute_query(query, "penalizaciones")

    # ================= MÉTODO PRINCIPAL =================
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extraer todos los datos siguiendo el orden de dependencias
        Retorna diccionario con DataFrames de todas las tablas
        """
        if not self.connect():
            return {}
        
        extracted_data = {}
        
        try:
            mode_msg = "INCREMENTAL" if self.incremental else "COMPLETA"
            if self.incremental and self.control:
                last_date = self.control.get_last_extraction_date()
                logger.info(f"=== EXTRACCIÓN {mode_msg} - Desde: {last_date} ===")
            else:
                logger.info(f"=== EXTRACCIÓN {mode_msg} ===")
            
            # 1. Tablas
            logger.info("--- Extrayendo Tablas ---")
            extracted_data['clientes'] = self.extract_clientes()
            extracted_data['empleados'] = self.extract_empleados()
            extracted_data['contratos'] = self.extract_contratos()
            extracted_data['proyectos'] = self.extract_proyectos()
            extracted_data['hitos'] = self.extract_hitos()
            extracted_data['tareas'] = self.extract_tareas()
            extracted_data['asignaciones'] = self.extract_asignaciones()
            extracted_data['pruebas'] = self.extract_pruebas()
            extracted_data['errores'] = self.extract_errores()
            extracted_data['riesgos'] = self.extract_riesgos()
            extracted_data['gastos'] = self.extract_gastos()
            extracted_data['penalizaciones'] = self.extract_penalizaciones()
            
            logger.info("=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
            
            # Resumen de extracción
            total_records = sum(len(df) for df in extracted_data.values())
            logger.info(f"Total de registros extraídos: {total_records}")
            
            for table_name, df in extracted_data.items():
                logger.info(f"  - {table_name}: {len(df)} registros")
            
            # Actualizar fecha de control si hay datos nuevos
            if self.incremental and self.control and total_records > 0:
                self.control.update_last_extraction_date()
                logger.info("Fecha de control incremental actualizada")
                
        except Exception as e:
            logger.error(f"Error durante la extracción: {str(e)}")
        finally:
            self.disconnect()
            
        return extracted_data


def extract_all(incremental: bool = True) -> Dict[str, pd.DataFrame]:
    """
    Función principal para extraer todos los datos
    
    Args:
        incremental: True, solo extrae registros nuevos
                    False, carga completa
    """
    extractor = SGPExtractor(incremental=incremental)
    return extractor.extract_all()

def reset_incremental_control():
    from utils.incremental_control import IncrementalControl
    control = IncrementalControl()
    control.reset_control()

def get_last_extraction_info():
    from utils.incremental_control import IncrementalControl
    control = IncrementalControl()
    return control.get_last_extraction_date()


if __name__ == "__main__":
    # Test de extracción
    data = extract_all()
    print("Extracción completada!")
    for table, df in data.items():
        print(f"{table}: {len(df)} registros")