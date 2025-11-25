#!/usr/bin/env python3
"""
Script para limpiar archivos temporales y de cache del proyecto ETL.

Uso:
    python clean_project.py

Este script elimina:
- Carpetas __pycache__
- Archivos .pyc
- Logs antiguos (opcional)
"""

import os
import shutil
import sys
from pathlib import Path

def clean_pycache():
    """Eliminar todas las carpetas __pycache__ del proyecto (excluyendo venv)."""
    project_root = Path(__file__).parent
    pycache_dirs = []
    
    # Buscar carpetas __pycache__ excluyendo venv
    for pycache_dir in project_root.rglob("__pycache__"):
        if "venv" not in str(pycache_dir):
            pycache_dirs.append(pycache_dir)
    
    if pycache_dirs:
        print(f"🧹 Eliminando {len(pycache_dirs)} carpetas __pycache__...")
        for pycache_dir in pycache_dirs:
            try:
                shutil.rmtree(pycache_dir)
                print(f"   ✓ Eliminado: {pycache_dir}")
            except Exception as e:
                print(f"   ❌ Error eliminando {pycache_dir}: {e}")
    else:
        print("✅ No se encontraron carpetas __pycache__ que limpiar")

def clean_pyc_files():
    """Eliminar archivos .pyc del proyecto (excluyendo venv)."""
    project_root = Path(__file__).parent
    pyc_files = []
    
    # Buscar archivos .pyc excluyendo venv
    for pyc_file in project_root.rglob("*.pyc"):
        if "venv" not in str(pyc_file):
            pyc_files.append(pyc_file)
    
    if pyc_files:
        print(f"🧹 Eliminando {len(pyc_files)} archivos .pyc...")
        for pyc_file in pyc_files:
            try:
                pyc_file.unlink()
                print(f"   ✓ Eliminado: {pyc_file}")
            except Exception as e:
                print(f"   ❌ Error eliminando {pyc_file}: {e}")
    else:
        print("✅ No se encontraron archivos .pyc que limpiar")

def clean_old_logs():
    """Limpiar logs antiguos (opcional)."""
    logs_dir = Path(__file__).parent / "logs"
    
    if not logs_dir.exists():
        print("✅ No existe directorio de logs")
        return
    
    log_files = list(logs_dir.glob("*.log"))
    if log_files:
        response = input(f"¿Eliminar {len(log_files)} archivos de log? (s/N): ").lower().strip()
        if response in ['s', 'si', 'sí', 'y', 'yes']:
            for log_file in log_files:
                try:
                    log_file.unlink()
                    print(f"   ✓ Eliminado: {log_file}")
                except Exception as e:
                    print(f"   ❌ Error eliminando {log_file}: {e}")
        else:
            print("   📝 Logs conservados")
    else:
        print("✅ No se encontraron archivos de log que limpiar")

def main():
    """Función principal de limpieza."""
    print("🚀 INICIANDO LIMPIEZA DEL PROYECTO ETL")
    print("=" * 50)
    
    try:
        # Limpiar carpetas __pycache__
        clean_pycache()
        print()
        
        # Limpiar archivos .pyc
        clean_pyc_files()
        print()
        
        # Limpiar logs (opcional)
        clean_old_logs()
        print()
        
        print("✅ LIMPIEZA COMPLETADA")
        print("=" * 50)
        
    except KeyboardInterrupt:
        print("\n⚠️  Limpieza interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error durante la limpieza: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()