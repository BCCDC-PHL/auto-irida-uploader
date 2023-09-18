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
    run_parent_dirs = config['run_parent_dirs']

    for run_parent_dir in run_parent_dirs:
        subdirs = os.scandir(run_parent_dir)

        for subdir in subdirs:
            run_id = subdir.name
            matches_miseq_regex = re.match(miseq_run_id_regex, run_id)
            matches_nextseq_regex = re.match(nextseq_run_id_regex, run_id)
            instrument_type = 'unknown'
            if matches_miseq_regex:
                instrument_type = 'miseq'
            elif matches_nextseq_regex:
                instrument_type = 'nextseq'
            ready_to_upload = os.path.exists(os.path.join(subdir, 'upload_complete.json'))
            upload_not_already_initiated = not os.path.exists(os.path.join(config['upload_staging_dir'], run_id))
            not_excluded = True
            if 'excluded_runs' in config:
                not_excluded = not run_id in config['excluded_runs']

            conditions_checked = {
                "is_directory": subdir.is_dir(),
                "matches_illumina_run_id_format": ((matches_miseq_regex is not None) or
                                                   (matches_nextseq_regex is not None)),
                "upload_not_already_initiated": upload_not_already_initiated,
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
        

def upload_run(config, run, upload_dir):
    """
    Initiate an analysis on one directory of fastq files.
    """
    run_id = run['sequencing_run_id']
    upload_successful = False
    upload_id = str(uuid.uuid4())

    upload_url = '/'.join([
        config['container_url'],
        upload_id,
        config['sas_token'],
    ])

    azcopy_command = [
        'azcopy',
        'cp',
        '--put-md5',
        '--recursive',
        '--follow-symlinks',
        '--output-type', 'json',
        '--from-to=LocalBlob',
        '--metadata=upload_id=' + upload_id,
        '--exclude-pattern=*NML_Upload_Finished*',
        upload_dir,
        upload_url,        
    ]

    logging.info(json.dumps({"event_type": "upload_started", "sequencing_run_id": run_id, "azcopy_command": " ".join(azcopy_command)}))
    try:
        subprocess.run(azcopy_command, capture_output=False, check=True)
        upload_successful = True
        time.sleep(5)
        logging.info(json.dumps({"event_type": "upload_completed", "sequencing_run_id": run_id, "azcopy_command": " ".join(azcopy_command)}))
    except subprocess.CalledProcessError as e:
        logging.error(json.dumps({"event_type": "upload_failed", "sequencing_run_id": run_id, "azcopy_command": " ".join(azcopy_command)}))

    if upload_successful:
        upload_complete_file_contents = {"action": "UPLOAD", "result": upload_successful, "job_id": upload_id}
        upload_complete_filename = upload_id + "-NML_Upload_Finished.json"
        upload_complete_path = os.path.join(upload_dir, upload_complete_filename)
        with open(upload_complete_path, "w", encoding="utf-8") as f:
            json.dump(upload_complete_file_contents, f)

        upload_url = config['container_url'] + config['sas_token']

        azcopy_command = [
            'azcopy',
            'cp',
            '--output-type', 'json',
            '--from-to=LocalBlob',
            upload_complete_path,
            upload_url,
        ]

        try:
            subprocess.run(azcopy_command, capture_output=False, check=True)
            logging.info(json.dumps({"event_type": "upload_confirmation_completed", "sequencing_run_id": run_id, "azcopy_command": " ".join(azcopy_command)}))
        except subprocess.CalledProcessError as e:
            logging.error(json.dumps({"event_type": "upload_confirmation_failed", "sequencing_run_id": run_id, "azcopy_command": " ".join(azcopy_command)}))
