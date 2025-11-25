# transform/transform_dim package
from .dim_clientes import transform as dim_clientes
from .dim_proyectos import transform as dim_proyectos
from .dim_empleados import transform as dim_empleados
from .dim_tiempo import transform as dim_tiempo
from .dim_gastos import transform as dim_gastos
from .dim_hitos import transform as dim_hitos
from .dim_tareas import transform as dim_tareas
from .dim_pruebas import transform as dim_pruebas

__all__ = [
    'dim_clientes','dim_proyectos','dim_empleados','dim_tiempo',
    'dim_gastos','dim_hitos','dim_tareas','dim_pruebas'
]
