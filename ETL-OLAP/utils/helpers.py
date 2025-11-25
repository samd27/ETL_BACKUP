# Comprobar la conexion a las bases de datos OLTP y DW
import mysql.connector
from config.db_config import DB_OLTP, DB_DW

def get_connection(db_type="OLTP"):
    cfg = DB_OLTP if db_type == "OLTP" else DB_DW
    conn = mysql.connector.connect(**cfg)
    return conn

def test_connection():
    for t in ("OLTP", "DW"):
        conn = get_connection(t)
        print(f"{t} conectado:", conn.is_connected())
        conn.close()
