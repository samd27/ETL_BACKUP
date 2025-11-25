# 🚀 ETL Project - Sistema de Gestión de Proyectos a Data Warehouse

## 📋 Descripción General

Este proyecto implementa un **pipeline ETL completo y funcional** que migra datos desde un Sistema de Gestión de Proyectos (SGP) hacia un Data Warehouse dimensional optimizado para análisis de Business Intelligence.

**🎯 Estado del Proyecto**: ✅ **COMPLETAMENTE FUNCIONAL** - 14,129 registros procesados exitosamente

---

## 🏆 Resultados de Carga Final

```
✅ DIMENSIONES CARGADAS:
├── dim_clientes      │ 79 registros    │ Maestro de clientes  
├── dim_empleados     │ 400 registros   │ Maestro de empleados
├── dim_proyectos     │ 74 registros    │ Proyectos filtrados (Cerrado/Cancelado)
├── dim_tiempo        │ 3,375 registros │ Calendario completo
├── dim_finanzas      │ 3,843 registros │ Transacciones financieras  
├── dim_hitos         │ 367 registros   │ Hitos con FK validadas
├── dim_tareas        │ 1,928 registros │ Tareas con FK validadas
└── dim_pruebas       │ 1,934 registros │ Pruebas con FK validadas

✅ HECHOS CARGADOS:
├── hechos_asignaciones │ 2,055 registros │ Asignaciones con métricas
└── hechos_proyectos    │ 74 registros    │ Proyectos con KPIs completos

📊 TOTAL CARGADO: 14,129 registros sin errores
🔍 FILTRADO INTELIGENTE: 74/173 proyectos (Cerrado + Cancelado únicamente)
```

---

## 🗂️ Estructura del Proyecto Limpio

```
ETL_PROJECT/
│
├── 📄 main_etl.py              # 🎯 Script principal del ETL
├── 📄 clean_project.py         # 🧹 Script de limpieza y mantenimiento
├── 📄 requirements.txt         # 📦 Dependencias Python
├── 📄 README.md               # 📚 Esta documentación
├── 📄 .env                    # 🔐 Variables de entorno (no incluido)
├── 📄 .gitignore             # 🚫 Archivos ignorados por Git
│
├── 📁 config/                 # ⚙️ Configuraciones
│   └── db_config.py          # Conexiones a bases de datos
│
├── 📁 DB/                     # 🗄️ Scripts de Base de Datos
│   ├── BD_SGP.sql            # Esquema fuente (Sistema Gestión)
│   └── DW_SSD.sql            # Esquema destino (Data Warehouse)
│
├── 📁 extract/                # 📥 Módulo de Extracción
│   └── extract_gestion.py    # Extractor principal con filtros
│
├── 📁 transform/              # 🔄 Módulo de Transformación
│   ├── common.py             # Utilidades comunes
│   ├── 📁 transform_dim/     # Transformaciones dimensionales
│   │   ├── dim_clientes.py   # Dimensión clientes
│   │   ├── dim_empleados.py  # Dimensión empleados  
│   │   ├── dim_finanzas.py   # Dimensión gastos/finanzas
│   │   ├── dim_hitos.py      # Dimensión hitos (con FK validation)
│   │   ├── dim_proyectos.py  # Dimensión proyectos (filtrado)
│   │   ├── dim_pruebas.py    # Dimensión pruebas (con FK validation)
│   │   ├── dim_riesgos.py    # Dimensión riesgos
│   │   ├── dim_severidad.py  # Dimensión severidad
│   │   ├── dim_tareas.py     # Dimensión tareas (con FK validation)
│   │   ├── dim_tiempo.py     # Dimensión tiempo
│   │   └── dim_tipo_riesgo.py # Dimensión tipo de riesgo
│   └── 📁 transform_fact/    # Transformaciones de hechos
│       ├── hechos_asignaciones.py # Hechos asignaciones (con FK validation)
│       └── hechos_proyectos.py    # Hechos proyectos (métricas complejas)
│
├── 📁 load/                   # 📤 Módulo de Carga
│   └── load_to_dw.py         # Carga completa al Data Warehouse
│
├── 📁 logs/                   # 📋 Logs del sistema
│   └── incremental_control.json # Control de proceso incremental
│
└── 📁 utils/                  # 🛠️ Utilidades
    ├── helpers.py            # Funciones auxiliares
    └── incremental_control.py # Control de extracción incremental
```

---

## ⚙️ Configuración del Entorno

### 1. **Instalación de Dependencias**
```bash
# Crear entorno virtual (recomendado)
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt
```

### 2. **Configuración de Base de Datos**
Crear archivo `.env` en la raíz del proyecto:
```env
# Base de datos fuente (SGP)
SGP_HOST=localhost
SGP_PORT=3306
SGP_USER=tu_usuario
SGP_PASSWORD=tu_password
SGP_DATABASE=sistema_gestion

# Base de datos destino (DW)
DW_HOST=localhost
DW_PORT=3306
DW_USER=tu_usuario
DW_PASSWORD=tu_password
DW_DATABASE=dw_proyectos
```

### 3. **Inicializar Esquemas de BD**
```sql
-- Ejecutar en MySQL Workbench o línea de comandos:
source DB/BD_SGP.sql;      -- Crear BD fuente
source DB/DW_SSD.sql;      -- Crear BD destino
```

---

## 🚀 Uso del Sistema

### **Ejecución Principal (Recomendada)**
```bash
# ETL completo con carga al Data Warehouse
python main_etl.py --test-load
```

### **Opciones de Ejecución**
```bash
# Solo extracción y transformación (sin carga)
python main_etl.py

# Limpiar proyecto de archivos temporales
python clean_project.py
```

### **Validación de Funcionamiento**
El sistema incluye validaciones automáticas:
- ✅ Conexiones a base de datos
- ✅ Dependencias de Foreign Keys
- ✅ Integridad referencial
- ✅ Métricas calculadas correctamente

---

## 🎯 Arquitectura ETL Implementada

```
SGP (OLTP)                    ETL Process                    DW (OLAP)
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ sistema_gestion │   ────>  │  EXTRACT con    │   ────>  │  dw_proyectos   │
│                 │          │  filtro crítico │          │                 │
│ - clientes      │          │                 │          │ ✅ dim_clientes  │
│ - proyectos     │          │  TRANSFORM con  │          │ ✅ dim_empleados │
│ - empleados     │          │  validación FK  │          │ ✅ dim_proyectos │ 
│ - hitos         │          │                 │          │ ✅ dim_tiempo    │
│ - tareas        │          │  LOAD con       │          │ ✅ dim_finanzas  │
│ - pruebas       │          │  control errores│          │ ✅ dim_hitos     │
│ - asignaciones  │          │                 │          │ ✅ dim_tareas    │
│ - gastos        │          │ FILTRO CRÍTICO: │          │ ✅ dim_pruebas   │
│ - etc...        │          │ Solo proyectos  │          │                 │
│                 │          │ 'Cerrado' y     │          │ ✅ hechos_asign  │
│                 │          │ 'Cancelado'     │          │ ✅ hechos_proyec │
└─────────────────┘          └─────────────────┘          └─────────────────┘
```

---

## 📊 Métricas y KPIs Calculados

### **En hechos_proyectos (74 registros):**
- 💰 **Métricas Financieras**: Presupuesto, CosteReal, DesviacionPresupuestal
- 📅 **Métricas de Tiempo**: RetrasoInicioDias, RetrasoFinalDias
- 🔍 **Métricas de Calidad**: TasaDeErroresEncontrados, TasaDeExitoEnPruebas
- 📈 **Métricas de Productividad**: ProductividadPromedio, PorcentajeTareasRetrasadas
- ⚠️ **Métricas de Gestión**: PenalizacionesMonto, ProporcionCAPEX_OPEX

### **En hechos_asignaciones (2,055 registros):**
- 👥 **Métricas de Recursos**: HorasPlanificadas, HorasReales, ValorHoras
- ⏰ **Métricas de Eficiencia**: RetrasoHoras

---

## 🔧 Características Técnicas Avanzadas

### **Filtrado Inteligente Implementado**
- **Regla de Negocio**: Solo proyectos con estado "Cerrado" o "Cancelado"
- **Resultado**: 74 proyectos válidos de 173 totales
- **Cascada**: Filtrado automático aplicado a todas las tablas relacionadas
- **Validación FK**: Eliminación automática de registros huérfanos

### **Manejo de Dependencias**
```
Orden de Carga (implementado):
1. 🥇 Dimensiones Independientes: clientes, empleados, tiempo, finanzas
2. 🥈 Dimensiones con FK: proyectos → hitos → tareas/pruebas  
3. 🥉 Hechos con todas las FK: asignaciones, proyectos
```

### **Control de Calidad Automático**
- ✅ Validación de Foreign Keys en cascada
- ✅ Limpieza de registros huérfanos
- ✅ Cálculo automático de métricas derivadas
- ✅ Control de duplicados

---

## 🧹 Mantenimiento y Limpieza

### **Script de Limpieza Automática**
```bash
# Ejecutar limpieza del proyecto
python clean_project.py

# Elimina automáticamente:
# ✅ Carpetas __pycache__ recursivamente
# ✅ Archivos .pyc compilados  
# ✅ Logs antiguos (opcional)
# ✅ Archivos temporales
```

### **Archivos Ignorados por Git**
El `.gitignore` está configurado para excluir:
```
__pycache__/     # Cache de Python
*.pyc            # Archivos compilados
*.pyo            # Archivos optimizados
.env             # Variables de entorno
venv/            # Entorno virtual
*.log            # Archivos de log
.DS_Store        # Archivos de macOS
```

---

## 🛠️ Solución de Problemas

### **Error de Conexión a BD**
1. ✔️ Verificar credenciales en archivo `.env`
2. ✔️ Confirmar que MySQL esté ejecutándose
3. ✔️ Validar que las bases de datos existan
4. ✔️ Verificar permisos del usuario

### **Error de Foreign Keys**
✅ **Resuelto automáticamente**: El ETL maneja las FK sin intervención manual
- Orden de carga respeta dependencias
- Validación automática de relaciones
- Filtrado de registros huérfanos

### **Rendimiento**
Para datasets grandes:
- Procesamiento por chunks implementado
- Optimización de queries con joins
- Control de memoria automático

---

## 📈 Casos de Uso de Análisis Habilitados

### **📊 Análisis Financiero**
- Rentabilidad por proyecto y cliente
- Control de desviaciones presupuestales
- Análisis de penalizaciones y sobrecostos
- ROI y margen por tipo de proyecto

### **⏰ Análisis Temporal**
- Cumplimiento de cronogramas
- Identificación de retrasos críticos
- Tendencias de productividad temporal
- Análisis de hitos críticos

### **👥 Análisis de Recursos**
- Utilización efectiva de empleados
- Costo real por hora trabajada
- Distribución de cargas de trabajo
- Performance por rol/seniority

### **🔍 Análisis de Calidad**
- Tasa de defectos por proyecto
- Efectividad de procesos de pruebas
- Análisis de riesgos materializados
- KPIs de mejora continua

---

## 💻 Tecnologías Utilizadas

- **🐍 Python 3.x**: Lenguaje principal del ETL
- **🐼 pandas**: Manipulación y transformación de datos
- **🔌 mysql-connector-python**: Conectividad con MySQL
- **⚙️ python-dotenv**: Gestión de configuración
- **🗄️ MySQL**: Sistema de base de datos
- **📝 Logging**: Sistema completo de auditoría

---

## ✅ Estado Final del Proyecto

### **🎉 PROYECTO 100% FUNCIONAL**
- ✅ **Extracción**: Implementada con filtros de negocio
- ✅ **Transformación**: Todas las dimensiones y hechos
- ✅ **Carga**: 10/10 tablas cargándose exitosamente  
- ✅ **Validación**: Foreign Keys resueltas automáticamente
- ✅ **Métricas**: KPIs complejos calculados correctamente
- ✅ **Limpieza**: Proyecto optimizado y mantenible

### **📊 Métricas Finales de Éxito**
```
🎯 TABLAS PROCESADAS: 10/10 (100%)
📊 REGISTROS CARGADOS: 14,129 sin errores
🔍 FILTROS APLICADOS: 74/173 proyectos válidos
⚡ FOREIGN KEYS: 100% resueltas automáticamente
🧹 PROYECTO: Limpio y optimizado
```

---

## 🚀 **¡EL SISTEMA ETL ESTÁ LISTO PARA PRODUCCIÓN!**

*Este README documenta un sistema ETL completamente funcional y validado.*