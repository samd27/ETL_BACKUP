import pandas as pd
from typing import Dict, Any, List
import matplotlib.pyplot as plt
import seaborn as sns

def generar_reporte_ejecutivo(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Genera un reporte ejecutivo completo con KPIs principales
    """
    reporte = {
        'fecha_generacion': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_proyectos': len(df),
        'clientes_activos': df['CodigoClienteReal'].nunique(),
    }
    
    # Métricas financieras
    reporte['metricas_financieras'] = {
        'presupuesto_total': df['Presupuesto'].sum(),
        'costo_real_total': df['CosteReal'].sum(),
        'desviacion_total': df['DesviacionPresupuestal'].sum(),
        'roi_promedio': ((df['Presupuesto'] - df['CosteReal']) / df['Presupuesto'] * 100).mean(),
        'eficiencia_presupuestal': (df['Presupuesto'] / df['CosteReal']).mean()
    }
    
    # Métricas operacionales
    reporte['metricas_operacionales'] = {
        'productividad_promedio': df['ProductividadPromedio'].mean(),
        'tasa_exito_promedio': df['TasaDeExitoEnPruebas'].mean(),
        'porcentaje_tareas_retrasadas': df['PorcentajeTareasRetrasadas'].mean(),
        'porcentaje_hitos_retrasados': df['PorcentajeHitosRetrasados'].mean()
    }
    
    # Distribución por estados
    reporte['distribucion_estados'] = df['Estado'].value_counts().to_dict()
    
    # Top clientes por presupuesto
    reporte['top_clientes'] = (df.groupby('CodigoClienteReal')['Presupuesto']
                              .sum().sort_values(ascending=False).head(5).to_dict())
    
    # Proyectos por categoría de presupuesto
    reporte['distribucion_presupuesto'] = df['CategoriaPresupuesto'].value_counts().to_dict()
    
    # Proyectos por año
    reporte['proyectos_por_anio'] = df['AnioInicio'].value_counts().sort_index().to_dict()
    
    return reporte

def generar_dashboard_olap(df: pd.DataFrame, guardar_archivo: bool = False):
    """
    Genera un dashboard visual con gráficos OLAP principales
    """
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Dashboard OLAP - Análisis de Proyectos', fontsize=16, fontweight='bold')
    
    # 1. Distribución por Estado
    estado_counts = df['Estado'].value_counts()
    axes[0, 0].pie(estado_counts.values, labels=estado_counts.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Distribución por Estado')
    
    # 2. Presupuesto vs Costo Real por Cliente (Top 10)
    top_clientes = df.groupby('CodigoClienteReal').agg({
        'Presupuesto': 'sum',
        'CosteReal': 'sum'
    }).sort_values('Presupuesto', ascending=False).head(10)
    
    top_clientes.plot(kind='bar', ax=axes[0, 1])
    axes[0, 1].set_title('Top 10 Clientes - Presupuesto vs Costo Real')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. Productividad por Categoría de Calidad
    prod_calidad = df.groupby('CategoriaCalidad')['ProductividadPromedio'].mean()
    prod_calidad.plot(kind='bar', ax=axes[0, 2], color='skyblue')
    axes[0, 2].set_title('Productividad Promedio por Categoría de Calidad')
    axes[0, 2].tick_params(axis='x', rotation=45)
    
    # 4. Evolución temporal de proyectos
    proyectos_anio = df['AnioInicio'].value_counts().sort_index()
    proyectos_anio.plot(kind='line', marker='o', ax=axes[1, 0])
    axes[1, 0].set_title('Evolución Temporal de Proyectos')
    axes[1, 0].set_xlabel('Año')
    axes[1, 0].set_ylabel('Cantidad de Proyectos')
    
    # 5. Heatmap: Cliente vs Categoría Presupuesto
    pivot_heatmap = pd.crosstab(df['CodigoClienteReal'], df['CategoriaPresupuesto'])
    sns.heatmap(pivot_heatmap, annot=True, fmt='d', ax=axes[1, 1], cmap='YlOrRd')
    axes[1, 1].set_title('Heatmap: Cliente vs Categoría Presupuesto')
    
    # 6. Boxplot: Desviación Presupuestal por Estado
    df.boxplot(column='DesviacionPresupuestal', by='Estado', ax=axes[1, 2])
    axes[1, 2].set_title('Distribución Desviación Presupuestal por Estado')
    axes[1, 2].set_xlabel('Estado')
    
    plt.tight_layout()
    
    if guardar_archivo:
        plt.savefig('dashboard_olap_proyectos.png', dpi=300, bbox_inches='tight')
        print("[OK] Dashboard guardado como 'dashboard_olap_proyectos.png'")
    
    plt.show()

def imprimir_reporte_ejecutivo(reporte: Dict[str, Any]):
    """
    Imprime el reporte ejecutivo en formato legible
    """
    print("=" * 80)
    print("[+] REPORTE EJECUTIVO - ANÁLISIS OLAP DE PROYECTOS")
    print("=" * 80)
    print(f"[*] Fecha de generación: {reporte['fecha_generacion']}")
    print(f"[*] Total de proyectos analizados: {reporte['total_proyectos']:,}")
    print(f"[*] Clientes activos: {reporte['clientes_activos']:,}")
    
    print("\n[+] MÉTRICAS FINANCIERAS:")
    print("-" * 40)
    mf = reporte['metricas_financieras']
    print(f"[+] Presupuesto total: ${mf['presupuesto_total']:,.2f}")
    print(f"[+] Costo real total: ${mf['costo_real_total']:,.2f}")
    print(f"[+] Desviación total: ${mf['desviacion_total']:,.2f}")
    print(f"[+] ROI promedio: {mf['roi_promedio']:.2f}%")
    print(f"[+] Eficiencia presupuestal: {mf['eficiencia_presupuestal']:.2f}x")
    
    print("\n[+] MÉTRICAS OPERACIONALES:")
    print("-" * 40)
    mo = reporte['metricas_operacionales']
    print(f"[+] Productividad promedio: {mo['productividad_promedio']:.2f} hrs/hito")
    print(f"[+] Tasa de éxito promedio: {mo['tasa_exito_promedio']:.2f}%")
    print(f"[+] Tareas retrasadas: {mo['porcentaje_tareas_retrasadas']:.2f}%")
    print(f"[+] Hitos retrasados: {mo['porcentaje_hitos_retrasados']:.2f}%")
    
    print("\n[+] DISTRIBUCIÓN POR ESTADOS:")
    print("-" * 40)
    for estado, cantidad in reporte['distribucion_estados'].items():
        porcentaje = cantidad / reporte['total_proyectos'] * 100
        print(f"{estado}: {cantidad:,} proyectos ({porcentaje:.1f}%)")
    
    print("\n[+] TOP 5 CLIENTES POR PRESUPUESTO:")
    print("-" * 40)
    for i, (cliente, presupuesto) in enumerate(reporte['top_clientes'].items(), 1):
        print(f"{i}. Cliente {cliente}: ${presupuesto:,.2f}")
    
    print("\n[+] PROYECTOS POR AÑO:")
    print("-" * 40)
    for anio, cantidad in sorted(reporte['proyectos_por_anio'].items()):
        print(f"{anio}: {cantidad:,} proyectos")

def exportar_cubos_excel(cubos: Dict[str, pd.DataFrame], nombre_archivo: str = 'cubos_olap.xlsx'):
    """
    Exporta todos los cubos OLAP a un archivo Excel con hojas separadas
    """
    with pd.ExcelWriter(nombre_archivo, engine='openpyxl') as writer:
        for nombre_cubo, cubo in cubos.items():
            # Limpiar nombre para hoja de Excel
            nombre_hoja = nombre_cubo.replace('_', ' ').title()[:31]  # Excel limit
            cubo.to_excel(writer, sheet_name=nombre_hoja)
            print(f"✅ Cubo '{nombre_cubo}' exportado a hoja '{nombre_hoja}'")
    
    print(f"[OK] Todos los cubos exportados a '{nombre_archivo}'")

def comparar_cubos(cubo1: pd.DataFrame, cubo2: pd.DataFrame, nombre1: str, nombre2: str):
    """
    Compara dos cubos OLAP y muestra las diferencias principales
    """
    print(f"\n🔍 COMPARACIÓN: {nombre1} vs {nombre2}")
    print("=" * 60)
    
    print(f"📏 Dimensiones {nombre1}: {cubo1.shape}")
    print(f"📏 Dimensiones {nombre2}: {cubo2.shape}")
    
    # Comparar índices
    indices_comunes = set(cubo1.index).intersection(set(cubo2.index))
    print(f"🔗 Índices comunes: {len(indices_comunes)}")
    
    # Comparar columnas numéricas si existen
    if len(cubo1.select_dtypes(include=['number']).columns) > 0:
        col_numerica = cubo1.select_dtypes(include=['number']).columns[0]
        if col_numerica in cubo2.columns:
            suma1 = cubo1[col_numerica].sum()
            suma2 = cubo2[col_numerica].sum()
            diferencia = suma2 - suma1
            print(f"[*] Suma {col_numerica} en {nombre1}: {suma1:,.2f}")
            print(f"[*] Suma {col_numerica} en {nombre2}: {suma2:,.2f}")
            print(f"📈 Diferencia: {diferencia:,.2f} ({diferencia/suma1*100:.1f}%)")

def generar_insights_automaticos(df: pd.DataFrame) -> List[str]:
    """
    Genera insights automáticos basados en el análisis de los datos
    """
    insights = []
    
    # Insight 1: Cliente más rentable
    rentabilidad_cliente = df.groupby('CodigoClienteReal')['DesviacionPresupuestal'].sum()
    cliente_mas_rentable = rentabilidad_cliente.idxmax()
    max_rentabilidad = rentabilidad_cliente.max()
    insights.append(f"[*] El cliente {cliente_mas_rentable} es el más rentable con ${max_rentabilidad:,.2f} bajo presupuesto")
    
    # Insight 2: Calidad vs Productividad
    corr_calidad_prod = df['TasaDeExitoEnPruebas'].corr(df['ProductividadPromedio'])
    if corr_calidad_prod > 0.5:
        insights.append(f"[*] Alta correlación positiva ({corr_calidad_prod:.2f}) entre calidad y productividad")
    elif corr_calidad_prod < -0.5:
        insights.append(f"[!] Correlación negativa ({corr_calidad_prod:.2f}) entre calidad y productividad")
    
    # Insight 3: Proyectos por encima/debajo de presupuesto
    sobre_presupuesto = (df['DesviacionPresupuestal'] < 0).sum()
    total_proyectos = len(df)
    porcentaje_sobre = sobre_presupuesto / total_proyectos * 100
    if porcentaje_sobre > 60:
        insights.append(f"[!] {porcentaje_sobre:.1f}% de proyectos superan el presupuesto")
    else:
        insights.append(f"✅ Solo {porcentaje_sobre:.1f}% de proyectos superan el presupuesto")
    
    # Insight 4: Tendencia temporal
    if 'AnioInicio' in df.columns and df['AnioInicio'].nunique() > 1:
        proyectos_por_anio = df.groupby('AnioInicio').size()
        if proyectos_por_anio.is_monotonic_increasing:
            insights.append("📈 Tendencia creciente en el número de proyectos por año")
        elif proyectos_por_anio.is_monotonic_decreasing:
            insights.append("📉 Tendencia decreciente en el número de proyectos por año")
    
    # Insight 5: Estado vs Calidad
    calidad_por_estado = df.groupby('Estado')['TasaDeExitoEnPruebas'].mean()
    if 'Cerrado' in calidad_por_estado and 'Cancelado' in calidad_por_estado:
        diff_calidad = calidad_por_estado['Cerrado'] - calidad_por_estado['Cancelado']
        if diff_calidad > 0.1:
            insights.append(f"✅ Proyectos cerrados tienen {diff_calidad:.1%} mejor calidad que cancelados")
    
    return insights