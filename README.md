# auto-irida-uploader

Automated upload of sequence data into an [IRIDA](https://irida.ca/) instance. Intended as a companion to the [auto-irida-upload-staging](https://github.com/BCCDC-PHL/auto-irida-upload-staging) tool.

## Usage

```
usage: auto-irida-uploader [-h] [-c CONFIG] [--log-level LOG_LEVEL]

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
  --log-level LOG_LEVEL
```

The config file should have the following format:

```
{
    "excluded_runs_list": "/path/to/excluded_runs.csv",
    "runs_to_upload_dir": "/path/to/irida_runs_to_upload",
    "scan_interval_seconds": 60,
    "irida_base_url": "https://your.irida.server.ca/irida/api/",
    "irida_username": "uploader",
    "irida_password": "s3cr3tpa$$w0rd",
    "irida_client_id": "uploader",
    "irida_client_secret": "cli3nts3cr3t",
    "parser": "directory"
}
```

...where the `excluded_runs_list` is simply a list of upload IDs that should be skipped when loading into IRIDA.:

```
81520c53-63d9-4ff4-91f4-061ecfe78807
f99862d1-0de9-43b7-a979-9d1a51423667
dc0ac452-6fff-48d0-8d2f-d8e06d6880cd
```

the `runs_to_upload_dir` should be the directory where BDIP-based uploads are deposited.
