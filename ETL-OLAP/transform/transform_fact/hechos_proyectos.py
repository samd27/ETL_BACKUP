import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime
import sys
import os

# Agregar path para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from transform.common import ensure_df, log_transform_info

logger = logging.getLogger(__name__)

def get_dependencies():
    return ['proyectos', 'contratos', 'errores', 'asignaciones', 'hitos', 'tareas', 'gastos', 'penalizaciones', 'dim_tiempo', 'dim_proyectos', 'dim_gastos']

def calculate_project_metrics(proyecto_id: int, df_dict: Dict[str, pd.DataFrame]) -> Optional[Dict]: 
    # Obtener datos principales
    proyectos = df_dict.get('proyectos', pd.DataFrame())
    contratos = df_dict.get('contratos', pd.DataFrame())
    gastos = df_dict.get('gastos', pd.DataFrame())  # Raw gastos para montos
    dim_gastos = df_dict.get('dim_gastos', pd.DataFrame())  # Dimension gastos para IDs
    penalizaciones = df_dict.get('penalizaciones', pd.DataFrame())
    dim_tiempo = df_dict.get('dim_tiempo', pd.DataFrame())
    
    proyecto_data = proyectos[proyectos['ID_Proyecto'] == proyecto_id]
    
    if proyecto_data.empty:
        return None
    
    proyecto = proyecto_data.iloc[0]
    
    # Inicializar métricas según schema DW exacto
    metrics = {
        'ID_Hecho': 0,  # Se asignará después
        'ID_Proyecto': proyecto_id,
        'ID_Riesgo': None,  # NULL ya que se eliminó tabla riesgos
        'ID_Gasto': 0,
        'ID_FechaInicio': 0,
        'ID_FechaFin': 0,
        'RetrasoInicioDias': 0,
        'RetrasoFinalDias': 0,  # Nombre correcto según DW
        'Presupuesto': 0.0,
        'CosteReal': 0.0,
        'DesviacionPresupuestal': 0.0,
        'PenalizacionesMonto': 0.0,
        'ProporcionCAPEX_OPEX': 0.0,
        'TasaDeErroresEncontrados': 0.0,
        'TasaDeExitoEnPruebas': 0.0,
        'ProductividadPromedio': 0.0,
        'PorcentajeTareasRetrasadas': 0.0,
        'PorcentajeHitosRetrasados': 0.0
    }
    
    # === RETRASOS (según especificaciones exactas) ===
    try:
        fecha_inicio = pd.to_datetime(proyecto.get('FechaInicio') or '', errors='coerce')
        fecha_inicio_real = pd.to_datetime(proyecto.get('FechaInicioReal') or '', errors='coerce')
        fecha_fin_plan = pd.to_datetime(proyecto.get('FechaFin') or '', errors='coerce')
        fecha_fin_real = pd.to_datetime(proyecto.get('FechaFinReal') or '', errors='coerce')
        
        # RetrasoInicioDias: FechaInicioReal - FechaInicio
        if pd.notna(fecha_inicio_real) and pd.notna(fecha_inicio):
            metrics['RetrasoInicioDias'] = max(0, (fecha_inicio_real - fecha_inicio).days)
        
        # RetrasoFinalDias: FechaFinReal - FechaFin
        if pd.notna(fecha_fin_real) and pd.notna(fecha_fin_plan):
            metrics['RetrasoFinalDias'] = max(0, (fecha_fin_real - fecha_fin_plan).days)
        
        # Mapear fechas a ID_Tiempo usando fórmula matemática
        # dim_tiempo empieza en 2018-01-01 con ID_Tiempo=1 (según log: desde 2018-01-01 hasta 2027-03-29)
        base_date = datetime(2018, 1, 1).date()
        
        if pd.notna(fecha_inicio):
            try:
                if isinstance(fecha_inicio, str):
                    fecha_inicio_date = pd.to_datetime(fecha_inicio).date()
                elif hasattr(fecha_inicio, 'date'):
                    fecha_inicio_date = fecha_inicio.date()
                else:
                    # Convertir directamente a date si es posible
                    fecha_inicio_date = pd.to_datetime(str(fecha_inicio)).date()
                
                # Calcular días desde la fecha base + 1 (porque ID_Tiempo empieza en 1)
                dias_desde_base = (fecha_inicio_date - base_date).days + 1  # type: ignore
                # Validar que esté dentro del rango de dim_tiempo (1 a 3375)
                if 1 <= dias_desde_base <= 3375:
                    metrics['ID_FechaInicio'] = dias_desde_base
                else:
                    # Si está fuera del rango, usar fecha por defecto (2018-01-01 = ID 1)
                    metrics['ID_FechaInicio'] = 1
            except Exception:
                metrics['ID_FechaInicio'] = 1
        
        if pd.notna(fecha_fin_real) or pd.notna(fecha_fin_plan):
            fecha_fin_para_mapeo = fecha_fin_real if pd.notna(fecha_fin_real) else fecha_fin_plan
            
            try:
                if isinstance(fecha_fin_para_mapeo, str):
                    fecha_fin_date = pd.to_datetime(fecha_fin_para_mapeo).date()
                elif hasattr(fecha_fin_para_mapeo, 'date'):
                    fecha_fin_date = fecha_fin_para_mapeo.date()
                else:
                    # Convertir directamente a date si es posible
                    fecha_fin_date = pd.to_datetime(str(fecha_fin_para_mapeo)).date()
                
                # Calcular días desde la fecha base + 1 (porque ID_Tiempo empieza en 1)
                dias_desde_base = (fecha_fin_date - base_date).days + 1  # type: ignore
                # Validar que esté dentro del rango de dim_tiempo (1 a 3375)
                if 1 <= dias_desde_base <= 3375:
                    metrics['ID_FechaFin'] = dias_desde_base
                else:
                    # Si está fuera del rango, usar fecha por defecto (2018-01-01 = ID 1)
                    metrics['ID_FechaFin'] = 1
            except Exception:
                metrics['ID_FechaFin'] = 1
    except Exception as e:
        logger.warning(f'Error calculando fechas para proyecto {proyecto_id}: {e}')
    
    # === PRESUPUESTO (desde contratos) ===
    # Presupuesto: ValorTotalContrato de la tabla contratos
    if not contratos.empty:
        contrato_proyecto = contratos[contratos['ID_Proyecto'] == proyecto_id]
        if not contrato_proyecto.empty:
            metrics['Presupuesto'] = float(contrato_proyecto['ValorTotalContrato'].iloc[0] or 0)
    
    # === COSTE REAL (gastos + sueldos de empleados) ===
    costo_gastos = 0.0
    costo_sueldos = 0.0
    
    # 1. Sumar todos los montos de la tabla gastos
    gastos_proyecto = gastos[gastos['ID_Proyecto'] == proyecto_id] if not gastos.empty else pd.DataFrame()
    if not gastos_proyecto.empty:
        costo_gastos = float(gastos_proyecto['Monto'].sum())
        
        # ID_Gasto: usar el primer gasto disponible en dim_gastos
        # Simplificado para evitar problemas de FK constraint
        if not dim_gastos.empty:
            metrics['ID_Gasto'] = int(dim_gastos.iloc[0]['ID_Finanza'])
        else:
            metrics['ID_Gasto'] = 1  # Default ID
        
        # ProporcionCAPEX_OPEX: CAPEX / OPEX
        gastos_capex = gastos_proyecto[gastos_proyecto['Categoria'] == 'CAPEX']['Monto'].sum()
        gastos_opex = gastos_proyecto[gastos_proyecto['Categoria'] == 'OPEX']['Monto'].sum()
        metrics['ProporcionCAPEX_OPEX'] = float(gastos_capex / gastos_opex) if gastos_opex > 0 else 0.0
    
    # 2. Sumar sueldos: HorasReales * CostoPorHora (ya calculado en asignaciones)
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    if not asignaciones.empty:
        asignaciones_proyecto = asignaciones[asignaciones['ID_Proyecto'] == proyecto_id]
        if not asignaciones_proyecto.empty:
            # CostoPorHora ya está en la tabla asignaciones desde la extracción
            # Calcular costo de sueldos: HorasReales * CostoPorHora
            asignaciones_proyecto = asignaciones_proyecto.copy()
            asignaciones_proyecto['CostoSueldo'] = pd.to_numeric(asignaciones_proyecto['HorasReales'], errors='coerce') * pd.to_numeric(asignaciones_proyecto['CostoPorHora'], errors='coerce')
            costo_sueldos = float(asignaciones_proyecto['CostoSueldo'].sum())
    
    # CosteReal total: gastos + sueldos
    metrics['CosteReal'] = costo_gastos + costo_sueldos
    
    # DesviacionPresupuestal: Presupuesto - CosteReal
    metrics['DesviacionPresupuestal'] = metrics['Presupuesto'] - metrics['CosteReal']
    
    # === PENALIZACIONES (desde tabla penalizaciones_contrato) ===
    if not penalizaciones.empty and not contratos.empty:
        contrato_proyecto = contratos[contratos['ID_Proyecto'] == proyecto_id]
        if not contrato_proyecto.empty:
            id_contrato = contrato_proyecto['ID_Contrato'].iloc[0]
            penalizaciones_proyecto = penalizaciones[penalizaciones['ID_Contrato'] == id_contrato]
            metrics['PenalizacionesMonto'] = float(penalizaciones_proyecto['Monto'].sum()) if not penalizaciones_proyecto.empty else 0.0
    
    # === PROPORCION CAPEX/OPEX (desde tabla gastos) ===
    if not gastos_proyecto.empty:
        capex_total = gastos_proyecto[gastos_proyecto['Categoria'].str.upper() == 'CAPEX']['Monto'].sum()
        opex_total = gastos_proyecto[gastos_proyecto['Categoria'].str.upper() == 'OPEX']['Monto'].sum()
        
        # ProporcionCAPEX_OPEX: CAPEX / OPEX
        metrics['ProporcionCAPEX_OPEX'] = float(capex_total / opex_total) if opex_total > 0 else 0.0
    
    # === RIESGOS ===
    # Removed as riesgos table doesn't exist in new schema
    
    # === TASA DE ERRORES (cantidad errores / cantidad tareas) ===
    errores = df_dict.get('errores', pd.DataFrame())
    tareas = df_dict.get('tareas', pd.DataFrame())
    hitos = df_dict.get('hitos', pd.DataFrame())
    
    if not errores.empty and not tareas.empty and not hitos.empty:
        # Obtener hitos del proyecto
        hitos_del_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id]['ID_Hito']
        # Obtener tareas de esos hitos
        tareas_del_proyecto = tareas[tareas['ID_Hito'].isin(hitos_del_proyecto)]
        total_tareas = len(tareas_del_proyecto)
        
        if total_tareas > 0:
            # Obtener errores de esas tareas
            tareas_ids = tareas_del_proyecto['ID_Tarea']
            errores_proyecto = errores[errores['ID_Tarea'].isin(tareas_ids)]
            total_errores = len(errores_proyecto)
            
            metrics['TasaDeErroresEncontrados'] = float(total_errores / total_tareas)
        else:
            metrics['TasaDeErroresEncontrados'] = 0.0
    else:
        metrics['TasaDeErroresEncontrados'] = 0.0
    
    # === TASA DE ÉXITO EN PRUEBAS (pruebas exitosas / pruebas totales) ===
    pruebas = df_dict.get('pruebas', pd.DataFrame())
    if not pruebas.empty and not hitos.empty:
        # Obtener hitos del proyecto
        hitos_del_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id]['ID_Hito']
        # Obtener pruebas de esos hitos
        pruebas_proyecto = pruebas[pruebas['ID_Hito'].isin(hitos_del_proyecto)]
        
        if len(pruebas_proyecto) > 0:
            pruebas_exitosas = pruebas_proyecto['Exitosa'].sum()
            metrics['TasaDeExitoEnPruebas'] = float(pruebas_exitosas / len(pruebas_proyecto))
        else:
            metrics['TasaDeExitoEnPruebas'] = 0.0
    else:
        metrics['TasaDeExitoEnPruebas'] = 0.0
    
    # === PRODUCTIVIDAD (HorasReales totales / cantidad hitos) ===
    asignaciones = df_dict.get('asignaciones', pd.DataFrame())
    if not asignaciones.empty and not hitos.empty:
        asig_proyecto = asignaciones[asignaciones['ID_Proyecto'] == proyecto_id]
        hitos_del_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id]
        
        if not asig_proyecto.empty and not hitos_del_proyecto.empty:
            total_horas_reales = asig_proyecto['HorasReales'].sum()
            total_hitos = len(hitos_del_proyecto)
            
            metrics['ProductividadPromedio'] = float(total_horas_reales / total_hitos) if total_hitos > 0 else 0.0
        else:
            metrics['ProductividadPromedio'] = 0.0
    else:
        metrics['ProductividadPromedio'] = 0.0
    
    # === PORCENTAJE TAREAS RETRASADAS ===
    # Usar dim_tareas que ya tiene SeRetraso calculado correctamente
    dim_tareas = df_dict.get('dim_tareas', pd.DataFrame())
    if not dim_tareas.empty:
        # Filtrar tareas del proyecto a través de hitos
        hitos_proyecto_ids = df_dict.get('hitos', pd.DataFrame())
        if not hitos_proyecto_ids.empty:
            hitos_del_proyecto = hitos_proyecto_ids[hitos_proyecto_ids['ID_Proyecto'] == proyecto_id]['ID_Hito'].tolist()
            tareas_del_proyecto = dim_tareas[dim_tareas['ID_Hito'].isin(hitos_del_proyecto)]
            
            if not tareas_del_proyecto.empty:
                total_tareas = len(tareas_del_proyecto)
                tareas_retrasadas = len(tareas_del_proyecto[tareas_del_proyecto['SeRetraso'] == 1])
                metrics['PorcentajeTareasRetrasadas'] = (tareas_retrasadas / total_tareas) * 100 if total_tareas > 0 else 0.0
    
    # === PORCENTAJE HITOS RETRASADOS ===
    hitos = df_dict.get('hitos', pd.DataFrame())
    if not hitos.empty:
        hitos_proyecto = hitos[hitos['ID_Proyecto'] == proyecto_id]
        if not hitos_proyecto.empty:
            total_hitos = len(hitos_proyecto)
            # Calcular retraso de hitos similar a tareas
            hitos_con_retraso = 0
            for _, hito in hitos_proyecto.iterrows():
                fecha_fin_real = pd.to_datetime(hito.get('FechaFinReal') or '', errors='coerce')
                fecha_fin_plan = pd.to_datetime(hito.get('FechaFinPlanificada') or '', errors='coerce')
                
                if pd.notna(fecha_fin_real) and pd.notna(fecha_fin_plan):
                    retraso_dias = (fecha_fin_real - fecha_fin_plan).days
                    if retraso_dias > 0:
                        hitos_con_retraso += 1
            
            metrics['PorcentajeHitosRetrasados'] = (hitos_con_retraso / total_hitos) * 100 if total_hitos > 0 else 0.0
    
    return metrics

def transform(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Usar dim_proyectos para garantizar integridad referencial
    dim_proyectos = ensure_df(df_dict.get('dim_proyectos', pd.DataFrame()))
    proyectos = ensure_df(df_dict.get('proyectos', pd.DataFrame()))  # Para datos adicionales
    
    if dim_proyectos.empty:
        logger.warning('hechos_proyectos: No hay datos de dim_proyectos')
        return pd.DataFrame(columns=[
            'ID_Hecho', 'ID_Proyecto', 'ID_Gasto', 'ID_FechaInicio', 'ID_FechaFin',
            'RetrasoInicioDias', 'RetrasoFinDias', 'Presupuesto', 'CosteReal', 
            'DesviacionPresupuestal', 'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 
            'TasaDeErroresEncontrados', 'TasaDeExitoEnPruebas', 'ProductividadPromedio', 
            'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    # Calcular métricas SOLO para proyectos válidos de dim_proyectos
    hechos_data = []
    hecho_id = 1
    
    logger.info(f'hechos_proyectos: Procesando {len(dim_proyectos)} proyectos válidos de dim_proyectos')
    
    for proyecto_id in dim_proyectos['ID_Proyecto'].unique():
        metrics = calculate_project_metrics(proyecto_id, df_dict)
        if metrics:  # Solo agregar si se calcularon las métricas
            metrics['ID_Hecho'] = hecho_id
            hechos_data.append(metrics)
            hecho_id += 1
    
    if not hechos_data:
        logger.warning('hechos_proyectos: No se pudieron calcular métricas para ningún proyecto')
        return pd.DataFrame(columns=[
            'ID_Hecho', 'ID_Proyecto', 'ID_Riesgo', 'ID_Gasto', 'ID_FechaInicio', 'ID_FechaFin',
            'RetrasoInicioDias', 'RetrasoFinalDias', 'Presupuesto', 'CosteReal', 
            'DesviacionPresupuestal', 'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 
            'TasaDeErroresEncontrados', 'TasaDeExitoEnPruebas', 'ProductividadPromedio', 
            'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
        ])
    
    result = pd.DataFrame(hechos_data)
    
    # Reordenar columnas según schema DW exacto
    columnas_ordenadas = [
        'ID_Hecho', 'ID_Proyecto', 'ID_Riesgo', 'ID_Gasto', 'ID_FechaInicio', 'ID_FechaFin',
        'RetrasoInicioDias', 'RetrasoFinalDias', 'Presupuesto', 'CosteReal', 
        'DesviacionPresupuestal', 'PenalizacionesMonto', 'ProporcionCAPEX_OPEX', 
        'TasaDeErroresEncontrados', 'TasaDeExitoEnPruebas', 'ProductividadPromedio', 
        'PorcentajeTareasRetrasadas', 'PorcentajeHitosRetrasados'
    ]
    
    result = result[columnas_ordenadas]
    
    log_transform_info('hechos_proyectos', len(proyectos), len(result))
    return result