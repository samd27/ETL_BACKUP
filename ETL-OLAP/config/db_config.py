"""
Database Configuration for ETL-OLAP Project
Contains connection parameters for OLTP (source) and DW (destination) databases.
"""

# OLTP Database Configuration (Source - Sistema de Gestión de Proyectos)
DB_OLTP = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "gestion_proyectos",
    "port": 3306
}

# Data Warehouse Configuration (Destination)
DB_DW = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "dw_proyectos",
    "port": 3306
}
