# SEO Data Pipeline

Workflow automatizado que extrae métricas diarias de Google Search Console (GSC) y Google Analytics 4 (GA4) y las carga en una Google Sheet. El workflow de GitHub Actions se ejecuta cada día a las 03:00 (Bogotá) y también podés lanzarlo manualmente desde la pestaña **Actions**.

## Cómo funciona

1. `pipeline/data_pipeline.py` determina la fecha a procesar. Por defecto usa dos días antes de la fecha de ejecución para anticipar la latencia de las APIs. Podés forzar otra fecha definiendo la variable de entorno `PIPELINE_TARGET_DATE` en formato `YYYY-MM-DD`.
2. `pipeline/gsc_connector.py` consulta Search Console y devuelve métricas por página (clicks, impresiones, CTR, posición). Si faltan credenciales o la API todavía no tiene datos publicados, genera un CSV de respaldo.
3. `pipeline/ga4_connector.py` obtiene métricas de GA4 por `pagePath` (usuarios, sesiones, duración media de sesión y tasa de rebote). Usa la Data API y retorna filas simuladas cuando no hay credenciales.
4. `pipeline/sheets_manager.py` realiza un upsert en Google Sheets y crea las pestañas `gsc_data_daily` y `ga4_data_daily` si no existen. La unión se hace por `date + url`, de modo que si vuelves a procesar el mismo día los datos se actualizan sin duplicados.

## Secrets necesarios

Configura estos secrets en el repositorio de GitHub para que el workflow pueda autenticarse y ubicar los recursos:

| Secret | Valor esperado | Notas |
| --- | --- | --- |
| `GSC_SERVICE_ACCOUNT_JSON` | JSON completo de la clave de service account con permisos en Search Console. Pegá el archivo entero (incluyendo saltos de línea) en el secret. La cuenta debe tener al menos permiso *Completo* sobre la propiedad objetivo. |
| `GA_SERVICE_ACCOUNT_JSON` | El mismo JSON (o uno equivalente) con acceso a la propiedad GA4 y la API de Google Analytics Data habilitada. Asignale un rol “Analyst” (o superior) dentro de GA4. |
| `SHEETS_SERVICE_ACCOUNT_JSON` | JSON del service account con permiso de edición sobre la hoja de cálculo. Comparte el spreadsheet con el `client_email` indicado en el archivo. |
| `SEO_MASTER_SPREADSHEET_ID` | ID de la hoja (cadena entre `/d/` y `/edit` en la URL de Google Sheets). |
| `GSC_SITE_URL` | Identificador exacto de la propiedad en Search Console. Usa `https://tu-sitio.com/` para propiedades URL-prefix o `sc-domain:tu-sitio.com` para propiedades de dominio. |
| `GA4_PROPERTY_ID` | Identificador de GA4 con el formato `properties/123456789`. |

## Ejecuciones manuales y solución de problemas

- Para reprocesar un día puntual, lanza el workflow mediante **workflow_dispatch** y fija `PIPELINE_TARGET_DATE` con la fecha deseada.
- Search Console suele liberar datos con hasta 48 horas de retraso. Si ejecutás el pipeline antes de que se publiquen, la pestaña de GSC puede quedar vacía temporalmente.
- Todos los módulos generan logs descriptivos (con emoticones) para que identifiques cualquier error rápidamente desde la consola de GitHub Actions.
