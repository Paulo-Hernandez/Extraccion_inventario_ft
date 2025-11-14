import xmlrpc.client
import pyodbc
import time

def cargar_configuracion(ruta):
    config = {}
    with open(ruta, 'r') as archivo:
        for linea in archivo:
            if '=' in linea:
                clave, valor = linea.strip().split('=', 1)
                config[clave.strip()] = valor.strip()
    return config

# Parámetros conexión Odoo
odoo_config = cargar_configuracion('odoo.txt')
url = odoo_config['url']
db = odoo_config['db']
username = odoo_config['username']
password = odoo_config['password']

# Parámetros conexión SQL Server
sql_config = cargar_configuracion('serverINV.txt')
server = sql_config['server']
database = sql_config['database']
username_sql = sql_config['username_sql']
password_sql = sql_config['password_sql']

# Conexión Odoo
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# Conexión SQL Server
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}')
cursor = conn.cursor()

# Inicio tiempo ejecución
start_time = time.time()

# Obtener productos desde Odoo
productos = models.execute_kw(db, uid, password,
                              'product.product', 'search_read',
                              [[]], {'fields': ['name', 'uom_name', 'categ_id','default_code','standard_price']})

count = 0
count_suc = 0

for producto in productos:
    id_producto = producto['id']
    nombre      = producto['name']
    unidad      = producto.get('uom_name', '')
    categoria   = producto.get('categ_id', [None, ''])[1]
    referencia  = producto.get('default_code', '')
    costo       = producto.get('standard_price', None)

    # 1) ¿Ya existe?
    cursor.execute("SELECT 1 FROM DIM_PRODUCTO WHERE ID = ?", (id_producto,))
    if cursor.fetchone():
        # 2a) Si existe, actualizar
        cursor.execute("""
            UPDATE DIM_PRODUCTO
            SET PRODUCTO  = ?,
                UNIDAD    = ?,
                CATEGORIA = ?,
                REFERENCIA = ?,
                COSTO      = ?
            WHERE ID = ?
        """, (nombre, unidad, categoria,referencia,costo, id_producto))
    else:
        # 2b) Si no existe, insertar
        cursor.execute("""
            INSERT INTO DIM_PRODUCTO (ID, PRODUCTO, UNIDAD, CATEGORIA,REFERENCIA,COSTO)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (id_producto, nombre, unidad, categoria,referencia,costo))

    count += 1



# Obtener companias desde Odoo
companies = models.execute_kw(db, uid, password,
                              'res.company', 'search_read',
                              [[]], {'fields': ['name', 'id']})

for comp in companies:
    id_comp = comp['id']
    nombre = comp['name']
    cursor.execute("SELECT 1 FROM DIM_ESTABLECIMIENTO WHERE ID = ?", (id_comp,))
    if cursor.fetchone():
        # 2a) Si existe, actualizar
        cursor.execute("""
                       UPDATE DIM_ESTABLECIMIENTO
                       SET SUCURSAL  = ?
                       WHERE ID = ?
                       """, (nombre,id_comp))
    else:
        # 2b) Si no existe, insertar
        cursor.execute("""
                       INSERT INTO DIM_ESTABLECIMIENTO (ID, SUCURSAL)
                       VALUES (?, ?)
                       """, (id_comp, nombre))
    count_suc += 1

conn.commit()
conn.close()

end_time = time.time()
total_time = round(end_time - start_time, 2)

print(f'Productos cargados: {count}')
print(f'Sucursales cargadas: {count_suc}')

print(f'Tiempo de ejecución: {total_time} segundos')

