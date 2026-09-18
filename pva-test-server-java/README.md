# PVAccess example server (Java)

A minimal PVAccess server built on the official EPICS Java stack
(`epics-pvaccess` / `epics-pvdata` / `epics-pvdatabase`, group `org.epics` on Maven Central).

It serves one channel, default name `EXAMPLE:SINECOUNTER`, with structure id
`example:SineCounter:1.0`:

```
example:SineCounter:1.0
    time_t timeStamp
        long secondsPastEpoch
        int nanoseconds
        int userTag
    int counter
    float value
```

* `timeStamp` — wall clock at update time, millisecond resolution
* `counter` — increments from zero, one per update
* `value` — `sin(2*pi*t/T)`, `t` taken from the timestamp, `T` = 10 s by default

The record is updated every 100 ms. Each update happens under the record lock inside
`beginGroupPut()`/`endGroupPut()`, so monitor clients see one coherent update per tick.

## Build

```
mvn package
```

Produces a self-contained `target/pva-test-server-1.0.0.jar`.

## Run

```
java -jar target/pva-test-server-1.0.0.jar [recordName] [sinePeriodSeconds]
```

For loopback-only testing:

```
EPICS_PVA_AUTO_ADDR_LIST=NO EPICS_PVA_ADDR_LIST=127.0.0.1 \
  java -jar target/pva-test-server-1.0.0.jar
```

Check with any PVA client, e.g. `pvget -m EXAMPLE:SINECOUNTER`.
