# Map Pulse to Timestamp

For SwissFEL, pulse-ids can be mapped to timestamps:

```bash
curl "https://data-api.psi.ch/api/4/map/pulse/sf-databuffer/PULSEID"
```

If the pulse can not be found, it returns http status 204.

If the pulse and the corresponding timestamp was found, it returns the normal status 200.
