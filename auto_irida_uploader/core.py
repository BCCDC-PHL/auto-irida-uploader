import datetime
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid

from typing import Iterator, Optional


def find_run_dirs(config, check_upload_complete=True):
    """
    Find sequencing run directories under the 'run_parent_dirs' listed in the config.

    :param config: Application config.
    :type config: dict[str, object]
    :param check_upload_complete: Check for presence of 'upload_complete.json' file.
    :type check_upload_complete: bool
    :return: Run directory. Keys: ['sequencing_run_id', 'path', 'instrument_type']
    :rtype: Iterator[Optional[dict[str, str]]]
    """
    miseq_run_id_regex = "\d{6}_M\d{5}_\d+_\d{9}-[A-Z0-9]{5}"
    nextseq_run_id_regex = "\d{6}_VH\d{5}_\d+_[A-Z0-9]{9}"
    runs_to_upload_dirs = [config['runs_to_upload_dir']]

    for runs_to_upload_dir in runs_to_upload_dirs:
        timestamped_subdirs = list(os.scandir(runs_to_upload_dir))

        for timestamped_subdir in timestamped_subdirs:
            run_subdirs = list(os.scandir(timestamped_subdir))
            for subdir in run_subdirs:
                run_id = subdir.name
                matches_miseq_regex = re.match(miseq_run_id_regex, run_id)
                matches_nextseq_regex = re.match(nextseq_run_id_regex, run_id)
                instrument_type = 'unknown'
                if matches_miseq_regex:
                    instrument_type = 'miseq'
                elif matches_nextseq_regex:
                    instrument_type = 'nextseq'
                ready_to_upload = True # Replace this with specific logic, need to coordinate with auto-irida-azure-upload tool.

                not_already_uploaded = True
                irida_uploader_status_path = os.path.join(subdir.path, 'irida_uploader_status.info')
                if os.path.exists(irida_uploader_status_path):
                    not_already_uploaded = False
            
                not_excluded = True
                if 'excluded_runs' in config:
                    not_excluded = not run_id in config['excluded_runs']

                conditions_checked = {
                    "is_directory": subdir.is_dir(),
                    "matches_illumina_run_id_format": ((matches_miseq_regex is not None) or
                                                       (matches_nextseq_regex is not None)),
                    "ready_to_upload": ready_to_upload,
                    "not_already_uploaded": not_already_uploaded,
                    "not_excluded": not_excluded,
                }

                if check_upload_complete:
                    conditions_checked["ready_to_upload"] = ready_to_upload

                conditions_met = list(conditions_checked.values())
                run = {}
                if all(conditions_met):
                    logging.info(json.dumps({"event_type": "run_directory_found", "sequencing_run_id": run_id, "run_directory_path": os.path.abspath(subdir.path)}))
                    run['path'] = os.path.abspath(subdir.path)
                    run['sequencing_run_id'] = run_id
                    run['instrument_type'] = instrument_type
                    yield run
                else:
                    logging.debug(json.dumps({"event_type": "directory_skipped", "run_directory_path": os.path.abspath(subdir.path), "conditions_checked": conditions_checked}))
                    yield None


def validate_samplelist(config, run):
    """
    Validate the sample list for a run.

    :param config: Application config.
    :type config: dict[str, object]
    :param run: Run directory. Keys: ['sequencing_run_id', 'path', 'instrument_type']
    :type run: dict[str, str]
    :return: True if the sample list is valid, False otherwise.
    :rtype: bool
    """
    samplelist_path = os.path.join(run['path'], 'SampleList.csv')
    samplelist_is_valid = False
    if os.path.exists(samplelist_path):
        with open(samplelist_path, 'r') as samplelist_file:
            header_line = samplelist_file.readline()
            header_line = header_line.strip()
            header_line = header_line.replace('"', '')
            header_line = header_line.replace("'", '')
            if header_line == '[Data]':
                samplelist_is_valid = True
                logging.info(json.dumps({"event_type": "samplelist_valid", "sequencing_run_id": run['sequencing_run_id'], "samplelist_path": samplelist_path}))
    else:
        logging.error(json.dumps({"event_type": "samplelist_missing", "sequencing_run_id": run['sequencing_run_id'], "samplelist_path": samplelist_path}))

    return samplelist_is_valid
            

def scan(config: dict[str, object]) -> Iterator[Optional[dict[str, object]]]:
    """
    Scanning involves looking for all existing runs and storing them to the database,
    then looking for all existing symlinks and storing them to the database.
    At the end of a scan, we should be able to determine which (if any) symlinks need to be created.

    :param config: Application config.
    :type config: dict[str, object]
    :return: A run directory to analyze, or None
    :rtype: Iterator[Optional[dict[str, object]]]
    """
    logging.info(json.dumps({"event_type": "scan_start"}))
    for run_dir in find_run_dirs(config):    
        yield run_dir
        

def upload_run(config, run):
    """
    Initiate an analysis on one directory of fastq files.
    """
    run_id = run['sequencing_run_id']
    upload_dir = run['path']
    upload_successful = False

    irida_uploader_command = [
        'irida-uploader',
        '--readonly',
        '--config_base_url', config['irida_base_url'],
        '--config_username', config['irida_username'],
        '--config_password', config['irida_password'],
        '--config_client_id', config['irida_client_id'],
        '--config_client_secret', config['irida_client_secret'],
        '--config_parser', config['parser'],
        '--directory', upload_dir,
    ]

    logging.info(json.dumps({"event_type": "upload_started", "sequencing_run_id": run_id, "irida_uploader_command": " ".join(irida_uploader_command)}))
    try:
        upload_result = subprocess.run(irida_uploader_command, capture_output=False, check=True, text=True)
        upload_successful = True
        logging.info(json.dumps({"event_type": "upload_completed", "sequencing_run_id": run_id, "irida_uploader_command": " ".join(irida_uploader_command)}))
    except subprocess.CalledProcessError as e:
        logging.error(json.dumps({"event_type": "upload_failed", "sequencing_run_id": run_id, "irida_uploader_command": " ".join(irida_uploader_command)}))

    if upload_successful:
        upload_parent_dir = os.path.dirname(upload_dir)
        if os.path.exists(upload_parent_dir):
            shutil.rmtree(upload_parent_dir)
            logging.info(json.dumps({"event_type": "directory_deleted", "directory_path": upload_parent_dir}))
