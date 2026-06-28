# REPORTS

Public batch report download portal.

## What it does

- Customers search by batch number and download matching reports.
- Admin uploads reports by batch number.
- An automation endpoint is reserved for future email-to-upload workflows.

## Render environment variables

- `ADMIN_PASSWORD`: password for internal upload page.
- `DATA_DIR`: persistent storage path. Use `/var/data` on Render with a Persistent Disk.
- `INGEST_TOKEN`: optional secret for future automatic upload integrations.

## URLs

- Customer portal: `/`
- Admin upload: `/admin`
- Health check: `/api/health`

