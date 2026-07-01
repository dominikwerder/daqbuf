# Backends

The list of available backends can be queried:

```bash
curl "https://data-api.psi.ch/api/4/backend/list"
```

Example output:
```json
{
  "backends_available": [
    {"name": "sf-archiver"},
    {"name": "sls-archiver"},
    {"name": "sf-databuffer"}
  ]
}
```
