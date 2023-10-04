# auto-irida-uploader

Automated upload of sequence data into an [IRIDA](https://irida.ca/) instance. Intended as a companion to the [auto-irida-azure-upload](https://github.com/BCCDC-PHL/auto-irida-azure-upload) tool.

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

...where the `excluded_runs_list` is simply a list of sequencing run IDs that should be excluded from uploads:

```
230812_M00123_126_000000000-AG4BE
230930_VH00234_45_AAE46WTN0
```

the `runs_to_upload_dir` should be the directory where azure-based uploads are deposited.
