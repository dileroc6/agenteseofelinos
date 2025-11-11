# SEO Data Pipeline

Automated workflow that extracts daily metrics from Google Search Console (GSC) and Google Analytics 4 (GA4) and pushes the results into a Google Sheet. The GitHub Actions workflow runs every day at 03:00 (Bogotá) and you can also trigger it manually from the Actions tab.

## How it works

1. `pipeline/data_pipeline.py` determines the processing date. By default it uses two days before the execution date (to account for API data latency). You can override the date by setting the environment variable `PIPELINE_TARGET_DATE` with a value in `YYYY-MM-DD` format.
2. `pipeline/gsc_connector.py` fetches page-level metrics from Search Console (clicks, impressions, CTR, position). If credentials are missing or the API has no data yet, it writes a CSV fallback.
3. `pipeline/ga4_connector.py` fetches page-path metrics from GA4 (users, sessions, average session duration, bounce rate). It uses the Data API and falls back to sample rows when credentials are unavailable.
4. `pipeline/sheets_manager.py` upserts both datasets into Google Sheets, creating the tabs `gsc_data_daily` and `ga4_data_daily` if they do not exist. Data is merged by `date + url` so reruns for the same day overwrite gracefully.

## Required secrets

Configure these secrets in the GitHub repository so the workflow can authenticate and locate resources:

| Secret | Expected value | Notes |
| --- | --- | --- |
| `GSC_SERVICE_ACCOUNT_JSON` | Full JSON for the service account key con permisos en Search Console. Copiá el archivo completo tal cual (incluyendo saltos de línea) y pegalo en el secret. La cuenta debe tener al menos permiso *Completo* sobre la propiedad objetivo. |
| `GA_SERVICE_ACCOUNT_JSON` | El mismo JSON (o uno con las mismas credenciales) con acceso a la propiedad GA4 y la API de Google Analytics Data habilitada. Asignale un rol “Analyst” (o superior) dentro de GA4. |
| `SHEETS_SERVICE_ACCOUNT_JSON` | El JSON del service account con permiso de edición sobre la hoja de cálculo. Comparte el spreadsheet con el `client_email` que figura en el archivo. |
| `SEO_MASTER_SPREADSHEET_ID` | The spreadsheet ID (the string between `/d/` and `/edit` in the Google Sheets URL). |
| `GSC_SITE_URL` | Exact property ID for Search Console. Use `https://example.com/` for URL-prefix properties or `sc-domain:example.com` for domain properties. |
| `GA4_PROPERTY_ID` | GA4 property identifier in the form `properties/123456789`. |

## Manual runs & troubleshooting

- To reprocess a specific day, trigger the workflow with the **workflow_dispatch** action and set `PIPELINE_TARGET_DATE` to the desired date.
- Search Console typically releases data 48 hours later. If you run the pipeline earlier, the GSC tab may stay empty until the API makes the data available.
- All modules emit descriptive logs (with emoticons) so you can quickly spot issues in the Actions console.
