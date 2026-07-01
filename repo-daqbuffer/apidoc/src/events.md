# Event Data

Event data can be fetched like this:

```bash
curl "https://data-api.psi.ch/api/4/events?backend=sf-databuffer&channelName=S10BC01-DBPM010:Q1&begDate=2024-02-15T12:41:00Z&endDate=2024-02-15T12:42:00Z"
```

Parameters:
- `backend`: the backend that the channel exists in, e.g. `sf-databuffer`.
- `channelName`: the name of the channel.
- `seriesId`: instead of a channel name, can specify the (unique within backend) series id
  as returned by channel search.
  Note that it is an error to provide both `channelName` and `seriesId`.
- `begDate`: start of the time range, inclusive. In ISO format e.g. `2024-02-15T12:41:00Z`.
- `endDate`: end of the time range, exclusive.
- `oneBeforeRange`: if set to `true` the reponse will in addition also contain the most recent event before the given range.
- `allowLargeResult=true` **DEPRECATED, will be rejected in the future**
  indicates that the client is prepared to accept also larger responses compared to
  what might be suitable for a typical browser. Please download large result sets as
  framed json or framed cbor streams, see below.

By default, events are returned as a json object.

Note: if the channel changes data type within the requested date range, then the
server will return values for that data type which covers the requested
date range best.

Note: if the server decides that the requested dataset gets "too large" then the response will contain
the key `continueAt` which indicates that the response is incomplete and that the caller should
issue another request with `begDate` as given by `continueAt`.


By default, or explicitly with `Accept: application/json` the response will be a json object.

Example response:
```json
{
  "tsAnchor": 1727336613,
  "tsMs": [
    3,
    13,
    23,
    33
  ],
  "tsNs": [
    80998,
    80999,
    81000,
    81001
  ],
  "pulseAnchor": 22238080000,
  "pulseOff": [
    998,
    999,
    1000,
    1001
  ],
  "values": [
    -0.005684227774617943,
    -0.0056660833356960184,
    -0.005697272133280464,
    -0.005831689871131955
  ]
}
```

The timestamp of each event is `TIMESTAMP = ANCHOR + MILLIS + NANOS`.

Note: the field `tsNs` may be omitted in the response if they are all zero.



## Events as framed JSON stream

To download larger amounts data as JSON it is recommended to use the `json-framed` content encoding.
Using this encoding, the server can send the requested events as a stream of json objects, where each
json object contains a batch of events.
This content encoding is triggered via the `Accept: application/json-framed` header in the request.

The returned body looks like:
```
[JSON-frame]
[JSON-frame]
[JSON-frame]
... etc
```

where each `[JSON-frame]` looks like:
```
[number of bytes N of the following json-encoded data, as ASCII-encoded number]
[newline]
[JSON object: N bytes]
[newline]
```

Note: "data" objects are currently identified by the presence of the `tss` key.
There can be other types of objects, like keepalive, log or statistics.


## Events as framed CBOR stream

The Concise Binary Object Representation (RFC 8949) can be a more compact option to transfer data.
Usage of the `Accept: application/cbor-framed` header in the request causes the api to return
the data as a stream of CBOR objects. Each CBOR-object will contain a batch of events.

The http body of the response then looks like this:

```
[CBOR-frame]
[CBOR-frame]
[CBOR-frame]
... etc
```

where each `[CBOR-frame]` looks like:
```
[length N of the following CBOR object: uint32 little-endian]
[reserved: 12 bytes of zero-padding]
[CBOR object: N bytes]
[padding: P zero-bytes, 0 <= P <= 7, such that (N + P) mod 8 = 0]
```

Most returned CBOR objects are data objects and look like this in equivalent json notation:
```json
{
  "tss": [1, 2, 3, 4],
  "values": [42, 60, 55, 20]
}
```
where `tss` is the array of timestamps and `values` the corresponding array of values.

Note: "data" objects are currently identified by the presence of the `tss` key.
There can be other types of objects, like keepalive, log or statistics.
