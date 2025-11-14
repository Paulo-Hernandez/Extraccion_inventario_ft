import xmlrpc.client
import pyodbc
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

new_period_ids      = []
new_product_ids     = []
new_sucursal_ids    = []
new_partner_ids     = []
new_tipo_mov_ids    = []
new_fact_ids        = []
updated_fact_ids    = []

CHILE_TZ = timezone(timedelta(hours=-4))

def send_email(config, subject, body):
    # Prepara el mensaje
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = config['from_addr']
    msg['To'] = ', '.join(config['to_addrs'].split(';'))

    # Conexión SSL/TLS implícito
    with smtplib.SMTP_SSL(config['smtp_server'],
                          int(config['smtp_port']),
                          timeout=10) as server:
        server.set_debuglevel(1)                     # opcional, para ver el diálogo SMTP
        server.login(config['smtp_user'], config['smtp_pass'])
        server.sendmail(config['from_addr'],
                        msg['To'].split(', '),
                        msg.as_string())

# Leer configuración de correo
def cargar_email_config(ruta):
    cfg = {}
    with open(ruta) as f:
        for line in f:
            if '=' in line:
                k, v = line.strip().split('=',1)
                cfg[k] = v
    return cfg

def get_period_dim_id(date_str, cursor, conn):
    """
    date_str: string ISO (p.ej. '2024-05-01T14:49:21+00:00' o '2024-05-01 14:49:21')
    cursor:   pyodbc.Cursor
    conn:     pyodbc.Connection
    """
    # 1) Parseamos el string a datetime
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

    # 2) Normalizamos a UTC si viene naïve, o dejamos su tzinfo
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    # 3) Convertimos a hora de Chile y lo hacemos naïve
    dt_chile = dt.astimezone(CHILE_TZ).replace(tzinfo=None)

    year, month, day, hour = dt_chile.year, dt_chile.month, dt_chile.day, dt_chile.hour

    # 4) Comprobamos si ya existe
    cursor.execute("""
        SELECT ID
          FROM DIM_PERIODO
         WHERE ANIO = ? AND MES = ? AND DIA = ? AND HORA = ?
    """, (year, month, day, hour))
    row = cursor.fetchone()
    if row:
        return row[0]

    # 5) No existe → generamos nuevo ID e insertamos
    cursor.execute("SELECT ISNULL(MAX(ID), 0) + 1 FROM DIM_PERIODO")
    new_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO DIM_PERIODO (ID, ANIO, MES, DIA, HORA)
        VALUES (?, ?, ?, ?, ?)
    """, (new_id, year, month, day, hour))
    conn.commit()

    new_period_ids.append(new_id)  # contador de periodos nuevos
    return new_id

def sync_dim_producto(
        prod_id: int,
        models,
        db: str,
        uid: int,
        password: str,
        cursor,
        conn
) -> int:
    """
    1) Lee desde Odoo los campos name, category, default_code y uom_name.
    2) Trunca cada valor al tamaño de la columna.
    3) Inserta o actualiza en DIM_PRODUCTO usando prod_id como clave.
    Devuelve siempre prod_id.

    - models/db/uid/password: tu conexión XML-RPC a Odoo
    - cursor/conn: tu conexión pyodbc a SQL Server
    """
    # 1) Leer desde Odoo
    prod = models.execute_kw(
        db, uid, password,
        'product.product', 'read',
        [[prod_id]],
        {'fields': ['name', 'categ_id', 'default_code', 'uom_id', 'standard_price']}
    )
    if not prod:
        raise ValueError(f"Producto {prod_id} no existe en Odoo")
    prod = prod[0]

    # Extraer valores
    name = prod.get('name') or ''
    default_code = prod.get('default_code') or ''
    # categ_id y uom_id vienen como tuplas (id, nombre)
    category_name = prod['categ_id'][1] if prod.get('categ_id') else ''
    uom_name = prod['uom_id'][1] if prod.get('uom_id') else ''
    costo = prod.get('standard_price', None)

    # 2) Truncar a tamaño de columnas
    name = name[:100]
    category_name = category_name[:50]
    default_code = default_code[:50]
    uom_name = uom_name[:50]

    # 3) Upsert en SQL Server
    cursor.execute(
        "SELECT 1 FROM DIM_PRODUCTO WHERE ID = ?",
        (prod_id,)
    )
    if cursor.fetchone():
        # UPDATE
        cursor.execute("""
                       UPDATE DIM_PRODUCTO
                       SET PRODUCTO   = ?,
                           UNIDAD     = ?,
                           CATEGORIA  = ?,
                           REFERENCIA = ?,
                           COSTO = ?
                       WHERE ID = ?
                       """, (
                           name,
                           uom_name,
                           category_name,
                           default_code,
                           costo,
                           prod_id,
                       ))
    else:
        # INSERT
        cursor.execute("""
                       INSERT INTO DIM_PRODUCTO (ID, PRODUCTO, UNIDAD, CATEGORIA, REFERENCIA,COSTO)
                       VALUES (?, ?, ?, ?, ?, ?)
                       """, (
                           prod_id,
                           name,
                           uom_name,
                           category_name,
                           default_code,
                           costo,
                       ))
        new_product_ids.append(prod_id)

    conn.commit()
    return prod_id

def sync_dim_sucursal(
    company_tuple: tuple,
    cursor,
    conn
) -> int:
    """
    Inserta o actualiza un registro en DIM_SUCURSAL.

    - company_tuple: tupla (company_id, sucursal_name) de Odoo, p.ej. (3, "FTFoods Sucursal La Dehesa")
    - cursor: pyodbc.Cursor
    - conn:   pyodbc.Connection

    Devuelve siempre el company_id.
    """
    # 1) Desempaquetar
    comp_id, sucursal = company_tuple
    # 2) Truncar al tamaño de la columna NVARCHAR(50)
    sucursal = sucursal[:50] if sucursal else None

    # 3) Comprobar si ya existe
    cursor.execute(
        "SELECT 1 FROM DIM_ESTABLECIMIENTO WHERE ID = ?",
        (comp_id,)
    )
    existe = cursor.fetchone() is not None

    if existe:
        # 4a) Actualizar nombre si cambió
        cursor.execute("""
            UPDATE DIM_ESTABLECIMIENTO
               SET SUCURSAL = ?
             WHERE ID = ?
        """, (sucursal, comp_id))
    else:
        # 4b) Insertar nuevo registro
        cursor.execute("""
            INSERT INTO DIM_ESTABLECIMIENTO (ID, SUCURSAL)
            VALUES (?, ?)
        """, (comp_id, sucursal))
        new_sucursal_ids.append(comp_id)

    # 5) Commit
    conn.commit()
    return comp_id

def sync_dim_cliente_proveedor(
    partner_tuple: tuple,
    models,
    db: str,
    uid: int,
    password: str,
    cursor,
    conn
) -> int | None:
    """
    Inserta o actualiza un partner en DIM_CLIENTES según
    item['picking_partner_id'] = (partner_id, partner_name).

    Devuelve el partner_id o None si partner_tuple es vacío.
    """
    # 1) Desempaquetar
    if not partner_tuple:
        return None
    partner_id, _ = partner_tuple

    # 2) Leer campos desde Odoo
    partner = models.execute_kw(
        db, uid, password,
        'res.partner', 'read',
        [[partner_id]],
        {'fields': ['name', 'phone', 'email', 'vat', 'street', 'street2', 'city']}
    )
    if not partner:
        return None
    p = partner[0]

    # 3) Construir valores y truncar a NVARCHAR(50)
    nombre    = (p.get('name')    or '')[:50]
    telefono  = (p.get('phone')   or '')[:50]
    correo    = (p.get('email')   or '')[:50]
    rut       = (p.get('vat')     or '')[:50]
    # Concatenar dirección: street, street2, city
    partes_dir = filter(None, [p.get('street'), p.get('street2'), p.get('city')])
    direccion = (", ".join(partes_dir))[:50]

    # 4) Upsert en SQL Server
    cursor.execute(
        "SELECT 1 FROM DIM_CLI_PROV WHERE ID = ?",
        (partner_id,)
    )
    if cursor.fetchone():
        # UPDATE
        cursor.execute("""
            UPDATE DIM_CLI_PROV
               SET NOMBRE    = ?,
                   TELEFONO  = ?,
                   CORREO    = ?,
                   RUT       = ?,
                   DIRECCION = ?
             WHERE ID = ?
        """, (
            nombre,
            telefono,
            correo,
            rut,
            direccion,
            partner_id
        ))
    else:
        # INSERT
        cursor.execute("""
            INSERT INTO DIM_CLI_PROV (
              ID, NOMBRE, TELEFONO, CORREO, RUT, DIRECCION
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            partner_id,
            nombre,
            telefono,
            correo,
            rut,
            direccion
        ))
        new_partner_ids.append(partner_id)

    conn.commit()
    return partner_id

def sync_dim_tipo_mov(
    item: dict,
    cursor,
    conn
) -> int:
    """
    Upsert en DIM_TIPO_MOV basándose en (reference, origen, destino).

    - item: el dict leído de stock.move.line, con claves
        'reference', 'location_id', 'location_dest_id'
      donde 'location_id' y 'location_dest_id' son tuplas (id, name).
    - cursor: pyodbc.Cursor
    - conn:   pyodbc.Connection

    Devuelve el ID existente o recién creado.
    """
    # 1) Extraer valores y usar el nombre de la ubicación
    reference       = item.get('reference') or ''
    origen          = item.get('location_id')[1]      if item.get('location_id')      else ''
    destino         = item.get('location_dest_id')[1] if item.get('location_dest_id') else ''

    # 2) Truncar a NVARCHAR(50)
    reference = reference[:50]
    origen    = origen[:50]
    destino   = destino[:50]

    # 3) Comprobar existencia
    cursor.execute("""
        SELECT ID
          FROM DIM_TIPO_MOV
         WHERE REFERENCIA = ?
           AND ORIGEN     = ?
           AND DESTINO    = ?
    """, (reference, origen, destino))
    row = cursor.fetchone()
    if row:
        return row[0]

    # 4) Nuevo ID manual (MAX+1 porque no es IDENTITY)
    cursor.execute("SELECT ISNULL(MAX(ID), 0) + 1 FROM DIM_TIPO_MOV")
    new_id = cursor.fetchone()[0]

    # 5) Insertar
    cursor.execute("""
        INSERT INTO DIM_TIPO_MOV (ID, REFERENCIA, ORIGEN, DESTINO)
        VALUES (?, ?, ?, ?)
    """, (new_id, reference, origen, destino))

    conn.commit()
    new_tipo_mov_ids.append(new_id)
    return new_id



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

all_fields = models.execute_kw(db, uid, password,
    'stock.move.line', 'fields_get', [], {'attributes': ['string', 'type']})
field_names = list(all_fields.keys())
fields = ['id','picking_id','company_id','product_id','quantity','date'
          ,'location_id','location_dest_id', 'picking_partner_id','reference']

with open('fechas.txt', 'r') as f:
    fechas = {}
    for line in f:
        if '=' in line:
            key, value = line.strip().split('=', 1)
            fechas[key.strip()] = value.strip()


inicio = fechas['Inicio']
fin   = fechas['Fin']

domain = [
    ['date', '>=', inicio],
    ['date', '<',  fin],
]

# Inicio tiempo ejecución
start_time = time.time()

ids = models.execute_kw(db, uid, password,
    'stock.move.line', 'search',
    [ domain ],
)

# 4. Leer todos los campos
data = models.execute_kw(db, uid, password,
    'stock.move.line', 'read', [ids, field_names,])

for item in data:
    # Identificadores
    id = item['id']

    # Periodo
    date_str = item['date']
    date_dim_id = get_period_dim_id(date_str, cursor, conn)

    # Producto
    prod_id = item['product_id'][0]
    # esto lee de Odoo y upserta en tu dimensión
    dim_prod_id = sync_dim_producto(
        prod_id,
        models, db, uid, password,
        cursor, conn
    )

    # Clientes/ proveedores
    dim_partner_id = sync_dim_cliente_proveedor(
        item.get('picking_partner_id'),
        models, db, uid, password,
        cursor, conn
    )

    # Establecimiento
    company_tuple = item['company_id']
    dim_sucursal_id = sync_dim_sucursal(
        company_tuple,
        cursor,
        conn
    )

    if dim_sucursal_id == 1:
        dim_sucursal_id = 2

    # Tipo de movimiento
    dim_tipo_mov_id = sync_dim_tipo_mov(item, cursor, conn)

    move_id = item['move_id'][0]

    # === COSTO REAL DESDE SVL (por movimiento y compañía/establecimiento) ===
    comp_id = company_tuple[0]  # company_id real de Odoo para filtrar SVL

    svl_ids = models.execute_kw(db, uid, password,
                                'stock.valuation.layer', 'search',
                                [[('stock_move_id', '=', move_id), ('company_id', '=', comp_id)]],
                                )

    costo_real_unit = None
    costo_real_tot = None

    if svl_ids:
        svls = models.execute_kw(db, uid, password,
                                 'stock.valuation.layer', 'read',
                                 [svl_ids, ['value', 'quantity']]
                                 )
        sum_val = sum(s.get('value', 0.0) for s in svls)
        sum_qty = sum(s.get('quantity', 0.0) for s in svls)

        if sum_qty and abs(sum_qty) > 0:
            # Unitario siempre positivo; total conserva el signo del movimiento
            costo_real_unit = abs(sum_val) / abs(sum_qty)

    # === FALLBACK si SVL no devuelve nada o qty=0 ===
    if costo_real_unit is None:
        # 1) intenta costo promedio del producto (a falta de historial por fecha)
        #    Si no te sirve, puedes leer product.standard_price
        prod = models.execute_kw(db, uid, password,
                                 'product.product', 'read',
                                 [[item['product_id'][0]]],
                                 {'fields': ['standard_price']}
                                 )
        std = prod[0].get('standard_price') or 0.0
        costo_real_unit = float(std)

    # --- total por línea ---
    qty_line = float(item['quantity'] or 0.0)

    if svl_ids and (sum_qty and abs(sum_qty) > 0):
        # Prorrateo exacto del valor del movimiento según la participación de la línea
        participacion = (abs(qty_line) / abs(sum_qty)) if sum_qty else 0.0
        costo_real_tot = (1 if qty_line >= 0 else -1) * abs(sum_val) * participacion
    else:
        # Fallback: unitario * cantidad de la línea
        costo_real_tot = (1 if qty_line >= 0 else -1) * abs(costo_real_unit) * abs(qty_line)

    # Ahora lees el coste unitario registrado en ese movimiento
    move = models.execute_kw(db, uid, password,
        'stock.move', 'read',
        [[move_id]],
        {'fields': ['price_unit']}
    )

    # Parametros Fact_inventario
    cantidad = item['quantity']
    precio_unitario = move[0]['price_unit']
    precio_total = precio_unitario * cantidad

    fact_id = item['id']
    tipo_mov_id = dim_tipo_mov_id
    establecimiento = dim_sucursal_id
    producto_id = dim_prod_id
    cli_prov_id = dim_partner_id
    periodo_id = date_dim_id
    cantidad = cantidad
    precio_comp = precio_unitario
    precio_tot = precio_total

    cursor.execute("SELECT 1 FROM FACT_INVENTARIO WHERE ID = ?", (fact_id,))
    if cursor.fetchone():
        # 2a) Si existe, lo actualizamos
        cursor.execute("""
                       UPDATE FACT_INVENTARIO
                       SET ID_TIPO_MOV        = ?,
                           ID_ESTABLECIMIENTO = ?,
                           ID_PRODUCTO        = ?,
                           ID_CLI_PROV        = ?,
                           ID_PERIODO         = ?,
                           CANTIDAD           = ?,
                           PRECIO_COMP        = ?,
                           PRECIO_TOT         = ?,
                           COSTO_REAL_UNIT    = ?,
                           COSTO_REAL_TOT     = ?
                       WHERE ID = ?
                       """, (
                           tipo_mov_id,
                           establecimiento,
                           producto_id,
                           cli_prov_id,
                           periodo_id,
                           cantidad,
                           precio_comp,
                           precio_tot,
                           costo_real_unit,
                           costo_real_tot,
                           fact_id
                       ))
        updated_fact_ids.append(fact_id)
        print(f"ID {fact_id} actualizado")
    else:
        # 2b) Si no existe, lo insertamos
        cursor.execute("""
                       INSERT INTO FACT_INVENTARIO (ID,
                                                    ID_TIPO_MOV,
                                                    ID_ESTABLECIMIENTO,
                                                    ID_PRODUCTO,
                                                    ID_CLI_PROV,
                                                    ID_PERIODO,
                                                    CANTIDAD,
                                                    PRECIO_COMP,
                                                    PRECIO_TOT,
                                                    COSTO_REAL_UNIT,
                                                    COSTO_REAL_TOT)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       """, (
                           fact_id,
                           tipo_mov_id,
                           establecimiento,
                           producto_id,
                           cli_prov_id,
                           periodo_id,
                           cantidad,
                           precio_comp,
                           precio_tot,
                           costo_real_unit,
                           costo_real_tot
                       ))
        new_fact_ids.append(fact_id)
        print(f"ID {fact_id} insertado")

# Al final del script, después de conn.commit()
conn.commit()

end_time = time.time()
total_time = round(end_time - start_time, 2)

summary = (
    "Resumen de ejecución de fact_inventario.py\n\n"
    f"Fecha de cargas  Desde:{inicio}  Hasta:{fin}\n"
    f"- Periodos insertados:      {len(new_period_ids)}\n"
    f"- Productos insertados:     {len(new_product_ids)}\n"
    f"- Sucursales insertadas:    {len(new_sucursal_ids)}\n"
    f"- Cli/Prov insertados:      {len(new_partner_ids)}\n"
    f"- TipoMov insertados:       {len(new_tipo_mov_ids)}\n\n"
    "Resumen FACT_INVENTARIO\n"
    f"- Insertados:               {len(new_fact_ids)}\n"
    f"- Actualizados:             {len(updated_fact_ids)}\n\n"
    f"Tiempo total de ejecución:  {total_time:.2f} segundos\n"
)

# Cargamos configuración de email y enviamos
email_cfg = cargar_email_config('email_config.txt')
send_email(
    email_cfg,
    subject="Resumen de ejecución de fact_inventario",
    body=summary
)

print("\n===== RESUMEN DE INSERCIONES =====")
print(f"Periodos   Nuevos: {len(new_period_ids)}")
print(f"Productos  Nuevos: {len(new_product_ids)}")
print(f"Sucursales Nuevos: {len(new_sucursal_ids)}")
print(f"Cli/Prov   Nuevos: {len(new_partner_ids)}")
print(f"TipoMov    Nuevos: {len(new_tipo_mov_ids)}")

print("\n===== RESUMEN FACT_INVENTARIO =====")
print(f"  Insertados:   {len(new_fact_ids)}")
print(f"  Actualizados: {len(updated_fact_ids)}")

print(f'Tiempo de ejecución: {total_time} segundos')

