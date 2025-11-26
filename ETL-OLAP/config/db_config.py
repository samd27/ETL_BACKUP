# config/db_config.py
from dotenv import load_dotenv
import os
import mysql.connector

# Carga variables desde archivo .env
load_dotenv()

# Configuración OLTP con soporte para TiDB Cloud
DB_OLTP = {
    "host": os.getenv("OLTP_HOST", "localhost"),
    "port": int(os.getenv("OLTP_PORT", "3306")),
    "user": os.getenv("OLTP_USER", "root"),
    "password": os. getenv("OLTP_PASS", ""),
    "database": os.getenv("OLTP_DB", "sistema_gestion")
}

# Agregar SSL si está habilitado (compatible con GitHub Actions)
if os. getenv("OLTP_SSL", "false").lower() == "true":
    DB_OLTP["ssl_disabled"] = False  # Habilitar SSL
    # NO usar ssl_verify_cert ni ssl_verify_identity en GitHub Actions

# Configuración DW con soporte para TiDB Cloud
DB_DW = {
    "host": os.getenv("DW_HOST", "localhost"),
    "port": int(os.getenv("DW_PORT", "3306")),
    "user": os.getenv("DW_USER", "root"),
    "password": os.getenv("DW_PASS", ""),
    "database": os.getenv("DW_DB", "dw_proyectos")
}

# Agregar SSL si está habilitado (compatible con GitHub Actions)
if os.getenv("DW_SSL", "false").lower() == "true":
    DB_DW["ssl_disabled"] = False  # Habilitar SSL

def get_dw_connection():
    """
    Crea y retorna una conexión al Data Warehouse
    """
    return mysql.connector.connect(**DB_DW)

def get_oltp_connection():
    """
    Crea y retorna una conexión a la base de datos OLTP
    """
    return mysql.connector.connect(**DB_OLTP)

def get_dw_sqlalchemy_url():
    """
    Retorna la URL de SQLAlchemy para el Data Warehouse
    """
    port = DB_DW.get('port', 3306)
    url = f"mysql+pymysql://{DB_DW['user']}:{DB_DW['password']}@{DB_DW['host']}:{port}/{DB_DW['database']}"
    
    # Agregar parámetros SSL si están configurados
    if os.getenv("DW_SSL", "false").lower() == "true":
        url += "?ssl=true"
    
    return url

def get_oltp_sqlalchemy_url():
    """
    Retorna la URL de SQLAlchemy para la base OLTP
    """
    port = DB_OLTP.get('port', 3306)
    url = f"mysql+pymysql://{DB_OLTP['user']}:{DB_OLTP['password']}@{DB_OLTP['host']}:{port}/{DB_OLTP['database']}"
    
    # Agregar parámetros SSL si están configurados
    if os.getenv("OLTP_SSL", "false"). lower() == "true":
        url += "?ssl=true"
    
    return url
