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

domain = [('supplier_rank', '>', 0)]      # o [('supplier', '=', True)] en Odoo 14+
fields = ['id', 'name', 'phone', 'email', 'street', 'vat']
proveedores = models.execute_kw(
    db, uid, password,
    'res.partner', 'search_read',
    [domain],
    {'fields': fields}
)
print(proveedores)
print(f"→ Encontrados {len(proveedores)} proveedores en Odoo.")
MAX_LEN = 50

# 2) Insertar ó actualizar en SQL Server
for p in proveedores:
    pid   = p['id']
    name  = p['name']
    if len(name) > MAX_LEN:
        name = name[:MAX_LEN]
    phone = p.get('phone')
    email = p.get('email')
    rut   = p.get('vat')
    addr  = p.get('street')

    cursor.execute("""
    IF EXISTS (SELECT 1 FROM dbo.DIM_CLI_PROV WHERE ID = ?)
      UPDATE dbo.DIM_CLI_PROV
         SET NOMBRE   = ?,
             TELEFONO = ?,
             CORREO   = ?,
             RUT      = ?,
             DIRECCION= ?
       WHERE ID = ?
    ELSE
      INSERT INTO dbo.DIM_CLI_PROV
        (ID, NOMBRE, TELEFONO, CORREO, RUT, DIRECCION)
      VALUES (?, ?, ?, ?, ?, ?)
    """, (
      # parámetros para UPDATE
      pid, name, phone, email, rut, addr, pid,
      # parámetros para INSERT
      pid, name, phone, email, rut, addr
    ))

conn.commit()
elapsed = time.time() - start_time
print(f"✔ {len(proveedores)} proveedores cargados/actualizados en {elapsed:.2f}s.")

# 3) Cerrar conexión
cursor.close()
conn.close()

end_time = time.time()
total_time = round(end_time - start_time, 2)

print(f'Tiempo de ejecución: {total_time} segundos')
