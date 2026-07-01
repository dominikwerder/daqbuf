# Daqbuffer

DAQ retrieval http API, contains:

* Retrieval http API to run on the nodes of our facilities (backends).
* Proxy to run on data-api.psi.ch for distribution of requests to facilities.


# Build

Tested on RHEL 7 and 8, CentOS 8.

If not yet done, see [Setup Toolchain](#setup-toolchain) first.

Then run in this directory:

```bash
cargo build --release
```

Executable will be placed by default at: `./target/release/daqbuffer`


# Download executable for AMD64 RHEL 7

The latest executable for RHEL 7 can be downloaded here from
<https://data-api.psi.ch/distri/daqbuffer-amd64-rhel7>
and also be found (e.g. via rsync) on host `data-api.psi.ch` at `/opt/distri/daqbuffer-amd64-rhel7`


# Deployment

## As Proxy

```bash
daqbuffer proxy --config <CONFIG.JSON>
```

Example config:
```json
{
  "name": "data-api.psi.ch",
  "listen": "0.0.0.0",
  "port": 8371,
  "backends": [
    {
      "name": "sls-archive",
      "url": "http://sls-archiver-api.psi.ch:8380"
    },
    {
      "name": "gls-archive",
      "url": "https://gls-data-api.psi.ch"
    }
  ],
  "backends_search": [
    {
      "name": "sf-databuffer",
      "url": "https://sf-data-api.psi.ch"
    },
    {
      "name": "sls-archive",
      "url": "http://sls-archiver-api.psi.ch:8380"
    },
    {
      "name": "gls-archive",
      "url": "https://gls-data-api.psi.ch"
    },
    {
      "name": "hipa-archive",
      "url": "https://hipa-data-api.psi.ch"
    },
    {
      "name": "proscan-archive",
      "url": "https://proscan-data-api.psi.ch"
    }
  ],
  "backends_pulse_map": [
    {
      "name": "sf-databuffer",
      "url": "https://sf-data-api.psi.ch"
    }
  ],
  "backends_status": [
    {
      "name": "sf-databuffer",
      "url": "https://sf-data-api.psi.ch"
    },
    {
      "name": "sf-archive",
      "url": "https://sf-archiver-api.psi.ch"
    },
    {
      "name": "sls-archive",
      "url": "http://sls-archiver-api.psi.ch:8380"
    },
    {
      "name": "gls-archive",
      "url": "https://gls-data-api.psi.ch"
    },
    {
      "name": "hipa-archive",
      "url": "https://hipa-data-api.psi.ch"
    },
    {
      "name": "proscan-archive",
      "url": "https://proscan-data-api.psi.ch"
    }
  ]
}```

## As Retrieval

```bash
daqbuffer retrieval --config <CONFIG.JSON>
```

Example config:
```json
{
  "name": "sf-daqbuf-21.psi.ch",
  "cluster": {
    "database": {
      "host": "sf-daqbuf-33.psi.ch",
      "name": "daqbuffer",
      "user": "daqbuffer",
      "pass": "daqbuffer"
    },
    "runMapPulse": true,
    "nodes": [
      {
        "host": "sf-daqbuf-21.psi.ch",
        "listen": "0.0.0.0",
        "port": 8380,
        "port_raw": 8390,
        "backend": "sf-databuffer",
        "cache_base_path": "{{databuffer_base_dir}}",
        "sf_databuffer": {
          "data_base_path": "{{databuffer_base_dir}}",
          "ksprefix": "{{databuffer_ks_prefix}}"
        }
      },
      "... more nodes here ..."
    ]
  }
}
```


# HTTP API docs

The documentation of the currently running service version is served by the service itself:

<https://data-api.psi.ch/api/4/docs/>


# Setup Toolchain

Install the Rust toolchain.
Quoting from <https://www.rust-lang.org/tools/install> the official installation method:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

This specifically requires a verified TLS connection for the download and then executes the installer.

Installation will by default be done only for the current user.
No superuser privileges are required.

You should have the commands `cargo` and `rustup` now available in your terminal.


# License

GNU General Public License version 3 or later.
