# Extracción de inventario FT Foods

Este repositorio contiene los scripts utilizados para sincronizar datos de inventario desde Odoo hacia un Data Warehouse en SQL Server y generar resúmenes automáticos de la carga. El flujo principal se ejecuta mediante `fact_inventario.py`, que consulta movimientos de inventario en Odoo, actualiza las tablas dimensionales y la tabla de hechos `FACT_INVENTARIO`, y finalmente envía un correo con el resumen de inserciones y actualizaciones.

## Contenido del repositorio

| Script | Descripción |
| --- | --- |
| `fact_inventario.py` | Proceso completo de extracción de movimientos, sincronización de dimensiones (periodo, producto, sucursal, cliente/proveedor, tipo de movimiento), carga de la tabla de hechos y envío del resumen por correo. |
| `productos.py` | Actualiza únicamente las dimensiones de productos (`DIM_PRODUCTO`) y sucursales (`DIM_ESTABLECIMIENTO`). |
| `proveedores.py` | Sincroniza la dimensión de clientes/proveedores (`DIM_CLI_PROV`). |
| `.spec` | Archivos de PyInstaller para empaquetar los scripts como ejecutables si se requiere distribución.

Además, el proyecto incluye archivos de configuración (`*.txt`) utilizados por los scripts para conectarse a los distintos servicios.

## Requisitos previos

1. **Python 3.9+** instalado en el servidor de ejecución.
2. Librerías de Python:
   ```bash
   pip install pyodbc
   ```
   (El resto de los módulos utilizados forman parte de la librería estándar).
3. **Controladores ODBC**: el servidor debe tener instalado *ODBC Driver 17 for SQL Server* o compatible para permitir la conexión con la base de datos destino.
4. Acceso de red a:
   - Instancia de Odoo expuesta por XML-RPC.
   - Servidor SQL Server con la base de datos `BI_INVENTARIO_FT_FOODS`.
   - Servidor SMTP configurado para el envío de correos.

## Archivos de configuración

Los scripts leen credenciales y parámetros desde archivos `.txt` ubicados en la raíz del proyecto. Cada archivo utiliza el formato `clave=valor` (una configuración por línea).

### `odoo.txt`
Parámetros de conexión contra Odoo.

```ini
url=https://<dominio-odoo>/
db=<nombre-base-datos>
username=<usuario>
password=<contraseña>
```

### `serverINV.txt`
Credenciales de conexión hacia SQL Server.

```ini
server=<host\instancia>
database=<nombre-base-datos>
username_sql=<usuario>
password_sql=<contraseña>
```

> **Nota:** el archivo no añade salto de línea al final; se recomienda editarlo asegurando que cada clave quede en su propia línea para evitar lecturas incorrectas.

### `fechas.txt`
Rango de fechas a procesar por `fact_inventario.py`.

```ini
Inicio=YYYY-MM-DD hh:mm:ss
Fin=YYYY-MM-DD hh:mm:ss
```

Los valores se utilizan para construir el dominio de búsqueda en Odoo (`date >= Inicio` y `date < Fin`).

### `email_config.txt`
Credenciales del servidor SMTP que enviará el resumen del proceso.

```ini
smtp_server=<host>
smtp_port=<puerto>
from_addr=<remitente>
to_addrs=<correo1;correo2>
smtp_user=<usuario>
smtp_pass=<contraseña>
```

El campo `to_addrs` acepta múltiples destinatarios separados por `;`.

### Otros archivos

- `server.txt` y `serverINV.txt`: mantener sincronizados si se trabaja con múltiples entornos.
- `campos_stock_picking.json`: respaldo de campos consultados desde Odoo (útil para depurar cambios futuros).

## Ejecución del proceso principal

1. **Configurar las credenciales** en los archivos `.txt` descritos anteriormente.
2. **Activar el entorno virtual** (opcional) y asegurarse de tener instaladas las dependencias.
3. Ejecutar el script principal:
   ```bash
   python fact_inventario.py
   ```
4. El script realizará:
   - Autenticación en Odoo vía XML-RPC.
   - Lectura de `stock.move.line` dentro del rango de fechas.
   - Upsert de las dimensiones (`DIM_PERIODO`, `DIM_PRODUCTO`, `DIM_ESTABLECIMIENTO`, `DIM_CLI_PROV`, `DIM_TIPO_MOV`).
   - Inserción/actualización de `FACT_INVENTARIO`.
   - Envío de un correo con el resumen de la ejecución.

Los mensajes de progreso se imprimen en consola, indicando IDs insertados o actualizados.

## Scripts auxiliares

- `python productos.py`: sincroniza únicamente productos y sucursales. Útil para precargar dimensiones o ejecutar cargas parciales.
- `python proveedores.py`: refresca la dimensión de proveedores sin correr el proceso completo de inventario.

## Resumen de resultados y correo

Al finalizar `fact_inventario.py` se construye un resumen con el número de registros insertados o actualizados por cada dimensión y por la tabla de hechos. El mismo mensaje se imprime en consola y se envía por correo a los destinatarios configurados. Verifique que las credenciales SMTP tengan permisos de envío y que el puerto corresponda al protocolo SSL/TLS requerido.

## Automatización

Para ejecutar el proceso de forma periódica:

- **Windows Task Scheduler**: crear una tarea programada que invoque `python fact_inventario.py` (o el ejecutable generado con PyInstaller) en el horario deseado.
- **Linux (cron)**: agregar una entrada al crontab del usuario, por ejemplo:
  ```cron
  0 6 * * * /usr/bin/python3 /ruta/al/proyecto/fact_inventario.py >> /var/log/fact_inventario.log 2>&1
  ```

Recuerde rotar las credenciales periódicamente y almacenar los archivos de configuración en ubicaciones seguras.

## Solución de problemas

- **Error de autenticación Odoo**: verificar `url`, `db`, `username` y `password` en `odoo.txt`.
- **Fallo de conexión SQL Server**: confirmar que el controlador ODBC está instalado y que el `server`/`database` sean accesibles.
- **Problemas con certificados SMTP**: ajustar el puerto o validar que la cuenta tenga habilitado el acceso SMTP.
- **Fechas sin resultados**: revisar el rango configurado en `fechas.txt`; el script aplica `date < Fin`, por lo que es conveniente definir el final como `23:59:59` del día deseado.

## Buenas prácticas

- No versionar archivos de configuración con credenciales reales en repositorios públicos.
- Mantener respaldos de los archivos `.spec` si se distribuyen ejecutables.
- Registrar los cambios relevantes en este `README.md` para facilitar el traspaso de conocimiento.

---

Ante cualquier duda o incidencia, documente el error observado (mensaje de consola o correo de resumen) y contacte al equipo de soporte para su revisión.
