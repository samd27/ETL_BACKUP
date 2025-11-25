# 📊 Sistema OLAP - Análisis Multidimensional de Proyectos

## 🎯 Descripción

Sistema OLAP (Online Analytical Processing) integrado con el Data Warehouse del sistema ETL para realizar análisis multidimensional de proyectos. Permite explorar datos desde múltiples perspectivas utilizando operaciones como Slice, Dice, Roll-up, Drill-down y Pivot.

## 🏗️ Arquitectura OLAP

```
Data Warehouse (OLTP)    →    Sistema OLAP (OLAP)    →    Reportes & Dashboards
┌─────────────────────┐       ┌─────────────────────┐       ┌─────────────────────┐
│ ✅ dim_clientes      │       │ 📊 Cubos OLAP        │       │ 📋 Reportes Ejecutivos│
│ ✅ dim_empleados     │  ───→ │ 🔍 Operaciones OLAP  │  ───→ │ 📈 Dashboards        │
│ ✅ dim_proyectos     │       │ 💡 Insights Auto     │       │ 📊 Visualizaciones  │
│ ✅ dim_tiempo        │       │ 📈 KPIs y Métricas   │       │ 💾 Exportación Excel│
│ ✅ hechos_proyectos  │       │                     │       │                     │
│ ✅ hechos_asignaciones│      │                     │       │                     │
└─────────────────────┘       └─────────────────────┘       └─────────────────────┘
```

## 📂 Estructura del Sistema

```
olap/
├── 📄 analisis_olap.py              # 🎯 Script principal OLAP
├── 📁 funciones/
│   ├── cargar_datos.py              # 📥 Carga de datos del DW
│   ├── crear_cubos.py               # 📊 Creación de cubos OLAP
│   ├── operaciones_olap.py          # 🔍 Operaciones OLAP (Slice, Dice, etc.)
│   └── reportes.py                  # 📋 Generación de reportes e insights
└── 📄 ../pipeline_completo.py       # 🚀 Pipeline integrado ETL + OLAP
```

## 🎲 Cubos OLAP Disponibles

### 1. **Cubo Base de Proyectos**
- **Dimensiones**: Cliente × Estado × Periodo
- **Medidas**: Presupuesto, Costo Real, Productividad Promedio
- **Uso**: Vista general de proyectos por cliente y tiempo

### 2. **Cubo Financiero**
- **Dimensiones**: Categoría Presupuesto × Tipo Desviación × Cliente
- **Medidas**: Cantidad proyectos, Presupuesto total, Costo real, Desviación
- **Uso**: Análisis financiero y control presupuestal

### 3. **Cubo Productividad-Calidad**
- **Dimensiones**: Categoría Productividad × Categoría Calidad × Estado
- **Medidas**: Cantidad proyectos, Presupuesto promedio, Desviación promedio
- **Uso**: Correlación entre productividad y calidad

### 4. **Cubo Temporal**
- **Dimensiones**: Año Inicio × Estado × Categoría Calidad
- **Medidas**: Cantidad proyectos, Tasa éxito, Productividad
- **Uso**: Tendencias temporales y evolución de calidad

### 5. **Cubo KPIs Ejecutivos**
- **Dimensiones**: Cliente × Periodo
- **Medidas**: ROI, Eficiencia Presupuestal, Indicador Calidad
- **Uso**: Dashboard ejecutivo de alto nivel

## 🔍 Operaciones OLAP Implementadas

### **Slice** (Corte)
Filtrar datos por una dimensión específica:
```python
# Proyectos de un cliente específico
slice_cliente = slice_por_cliente(df, cliente_id=87)

# Proyectos de un año específico
slice_anio = slice_por_anio(df, anio=2024)
```

### **Dice** (Dados)
Filtrar por múltiples dimensiones simultáneamente:
```python
# Proyectos grandes cerrados en últimos años
dice_resultado = dice_subset(
    df,
    estados=['Cerrado'],
    categorias_presupuesto=['Grande', 'Mega'],
    anios=[2024, 2025]
)
```

### **Roll-up** (Agregación)
Subir en la jerarquía dimensional:
```python
# Agregar por cliente
rollup_clientes = rollup_por_cliente(df)

# Agregación jerárquica: Total → Cliente → Estado → Categoría
rollup_jerarquico = rollup_jerarquico(df)
```

### **Drill-down** (Perforación)
Bajar en la jerarquía para ver más detalle:
```python
# Detalle de proyectos por cliente
detalle_cliente = drilldown_cliente_detallado(df, cliente_id=87)

# Drill-down temporal: Año → Trimestre → Mes
detalle_temporal = drilldown_temporal(df, nivel='mes')
```

### **Pivot** (Rotación)
Intercambiar dimensiones entre filas y columnas:
```python
# Cliente vs Estado
pivot_resultado = pivot_cliente_estado(df)

# Año vs Categoría de Calidad
pivot_temporal = pivot_anio_calidad(df)
```

## 📊 Dimensiones Analíticas

### **Dimensiones Principales**
- **CodigoClienteReal**: Identificador único del cliente
- **Estado**: Cerrado / Cancelado
- **AnioInicio / MesInicio**: Dimensiones temporales
- **CodigoProyecto**: Identificador del proyecto

### **Dimensiones Derivadas**
- **CategoriaPresupuesto**: Pequeño / Mediano / Grande / Mega
- **TipoDesviacion**: Sobre/Bajo/En Presupuesto
- **CategoriaProductividad**: Baja / Media / Alta / Muy Alta
- **CategoriaCalidad**: Baja / Media / Alta / Excelente
- **PeriodoInicio**: Año-Trimestre (ej: 2024-Q1)

### **Medidas Disponibles**
- **Financieras**: Presupuesto, CosteReal, DesviacionPresupuestal
- **Operacionales**: ProductividadPromedio, TasaDeExitoEnPruebas
- **Calidad**: PorcentajeTareasRetrasadas, PorcentajeHitosRetrasados
- **Derivadas**: ROI, EficienciaPresupuestal, IndicadorCalidad

## 🚀 Uso del Sistema

### **Ejecución Pipeline Completo**
```bash
# ETL + OLAP completo
python pipeline_completo.py

# Solo análisis OLAP (asume ETL ejecutado)
python pipeline_completo.py --solo-olap

# Solo ETL
python pipeline_completo.py --solo-etl
```

### **Ejecución OLAP Independiente**
```bash
# Análisis completo
python olap/analisis_olap.py

# Modo interactivo
python olap/analisis_olap.py --interactivo
```

### **Modo Interactivo**
```bash
python pipeline_completo.py --interactivo
```
Permite:
- ✅ Exploración interactiva de datos
- ✅ Slice y Dice personalizados
- ✅ Generación de reportes on-demand
- ✅ Consultas ad-hoc

## 📋 Salidas y Reportes

### **Archivos Generados**
1. **`analisis_olap_proyectos.xlsx`**
   - Todos los cubos OLAP en hojas separadas
   - Formato Excel para análisis en herramientas BI

2. **`dataset_olap_proyectos.csv`**
   - Dataset completo desnormalizado
   - Listo para importar en Power BI, Tableau, etc.

3. **`reporte_ejecutivo.txt`**
   - Métricas clave e insights automáticos
   - Resumen ejecutivo de alto nivel

### **Reporte Ejecutivo Incluye**
- 📊 **Métricas Financieras**: Presupuesto total, Costo real, ROI, Eficiencia
- ⚡ **Métricas Operacionales**: Productividad, Tasa de éxito, Retrasos
- 📈 **Distribuciones**: Por estado, cliente, categoría, año
- 🏆 **Rankings**: Top clientes, proyectos más rentables
- 💡 **Insights Automáticos**: Correlaciones y tendencias detectadas

### **Insights Automáticos**
El sistema genera automáticamente:
- 🏆 Cliente más rentable
- 📊 Correlación calidad-productividad
- 🚨 Porcentaje de proyectos sobre presupuesto
- 📈 Tendencias temporales
- ✅ Comparación calidad por estado

## 🎯 Casos de Uso Empresariales

### **1. Análisis Financiero**
```python
# Rentabilidad por cliente
cubo_financiero = cubo_financiero(df)

# Proyectos con mayor desviación presupuestal
slice_desviacion = dice_subset(df, categorias_presupuesto=['Grande', 'Mega'])
```

### **2. Control de Calidad**
```python
# Correlación productividad-calidad
cubo_prod_calidad = cubo_productividad_calidad(df)

# Proyectos de baja calidad para mejora
slice_baja_calidad = dice_subset(df, categorias_calidad=['Baja'])
```

### **3. Análisis Temporal**
```python
# Evolución de proyectos por año
rollup_temporal = rollup_por_anio(df)

# Drill-down: Año → Trimestre → Mes
detalle_temporal = drilldown_temporal(df, nivel='mes')
```

### **4. Dashboard Ejecutivo**
```python
# KPIs de alto nivel
cubo_kpis = cubo_kpis_ejecutivos(df)

# Reporte ejecutivo completo
reporte = generar_reporte_ejecutivo(df)
```

## 🔧 Personalización

### **Crear Cubo Personalizado**
```python
from funciones.crear_cubos import crear_cubo_personalizado

cubo_custom = crear_cubo_personalizado(
    df,
    dimensiones=['Cliente', 'Estado'],
    medidas=['Presupuesto', 'CosteReal'],
    columnas=['CategoriaCalidad'],
    agregaciones={'Presupuesto': 'sum', 'CosteReal': 'sum'}
)
```

### **Operación OLAP Personalizada**
```python
from funciones.operaciones_olap import operacion_personalizada

resultado = operacion_personalizada(
    df,
    operacion='rollup',
    dimensiones=['CodigoClienteReal', 'Estado'],
    medidas=['Presupuesto', 'ProductividadPromedio'],
    filtros={'AnioInicio': [2024, 2025]}
)
```

## 📈 Integración con Herramientas BI

### **Power BI**
1. Importar `dataset_olap_proyectos.csv`
2. Crear relaciones automáticas
3. Usar dimensiones derivadas para filtros
4. Crear visualizaciones con medidas calculadas

### **Tableau**
1. Conectar a archivo Excel `analisis_olap_proyectos.xlsx`
2. Cada hoja es un cubo OLAP independiente
3. Crear dashboards combinando múltiples cubos

### **Excel**
1. Abrir `analisis_olap_proyectos.xlsx`
2. Cada cubo está en una hoja separada
3. Crear tablas dinámicas y gráficos
4. Usar slicers para filtrado interactivo

## 🚨 Requisitos y Dependencias

### **Prerequisitos**
- ✅ ETL ejecutado exitosamente
- ✅ Data Warehouse poblado con datos
- ✅ Conexión a base de datos MySQL
- ✅ Archivo `.env` configurado

### **Dependencias Python**
- `pandas` - Manipulación de datos
- `numpy` - Operaciones numéricas
- `mysql-connector-python` - Conexión BD
- `matplotlib, seaborn` - Visualizaciones (opcional)
- `openpyxl` - Exportación Excel

## 🎉 Casos de Éxito

### **Ejemplo: Análisis Cliente Rentable**
```
🏆 Cliente 87 es el más rentable con $45,230.50 bajo presupuesto
📊 Análisis de 74 proyectos en 3 años
⚡ Productividad promedio: 420.15 hrs/hito
✅ Tasa de éxito: 78.5%
```

### **Ejemplo: Insight Calidad-Productividad**
```
✅ Alta correlación positiva (0.73) entre calidad y productividad
💡 Proyectos de alta calidad tienden a ser más productivos
🎯 Enfocar en mejora de procesos de calidad
```

## 🔮 Roadmap Futuro

- 🔄 **Cubos Incrementales**: Actualización automática con nuevos datos ETL
- 📱 **API REST**: Endpoints para consumo de cubos OLAP
- 🎨 **Dashboard Web**: Interfaz web interactiva
- 🤖 **ML Integration**: Predicciones basadas en análisis OLAP
- ☁️ **Cloud Support**: Integración con servicios cloud (AWS, Azure)

---

**¡El sistema OLAP está listo para transformar tus datos de proyectos en insights accionables!** 🚀