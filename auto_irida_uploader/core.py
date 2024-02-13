import csv
import datetime
import glob
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid

from typing import Iterator, Optional


def parse_samplelist(samplelist_path):
    """
    Parse a SampleList.csv file, and return a dict of dicts. Outer dict keys are library IDs.
    Inner dicts include keys: 'library_id', 'project_id', 'fastq_forward_filename', 'fastq_reverse_filename'

    :param samplelist_path: Path to SampleList.csv file
    :type samplelist_path: str
    :return: SampleList data, indexed by library ID.
    :rtype: dict[dict[str, str]]
    """
    samplelist_data_by_library_id = {}
    with open(samplelist_path, 'r') as f:
        next(f) # skip [Data] header line
        reader = csv.DictReader(f, dialect='unix')
        for row in reader:
            library_id = row['Sample_Name']
            library = {
                'library_id': library_id,
                'project_id': row['Project_ID'],
                'fastq_forward_filename': row['File_Forward'],
                'fastq_reverse_filename': row['File_Reverse'],
            }
            samplelist_data_by_library_id[library_id] = library

    return samplelist_data_by_library_id


def collect_md5_checksums(run_dir, samplelist_data_by_library_id):
    """
    Collect md5 checksums for all fastq files in run_dir.

    :param run_dir: Path to run dir to be uploaded
    :type run_dir: str
    :param samplelist_data: Parsed SampleList.csv file
    :type samplelist_data:
    :return:
    :rtype:
    """
    md5_checksums_by_library_id = {}
    for library_id, samplelist_data in samplelist_data_by_library_id.items():
        fastq_forward_path = os.path.join(run_dir, samplelist_data['fastq_forward_filename'])
        fastq_forward_realpath = os.path.realpath(fastq_forward_path)
        fastq_forward_md5 = None
        with open(fastq_forward_realpath, 'rb') as f:
            fastq_forward_hash = hashlib.md5()
            while chunk := f.read(8192):
                fastq_forward_hash.update(chunk)
            fastq_forward_md5 = fastq_forward_hash.hexdigest()
        fastq_reverse_path = os.path.join(run_dir, samplelist_data['fastq_reverse_filename'])
        fastq_reverse_realpath = os.path.realpath(fastq_reverse_path)
        fastq_reverse_md5 = None
        with open(fastq_reverse_realpath, 'rb') as f:
            fastq_reverse_hash = hashlib.md5()
            while chunk := f.read(8192):
                fastq_reverse_hash.update(chunk)
            fastq_reverse_md5 = fastq_reverse_hash.hexdigest()

        md5_checksums_by_library_id[library_id] = {
            'library_id': library_id,
            'fastq_forward_md5': fastq_forward_md5,
            'fastq_reverse_md5': fastq_reverse_md5,
        }

    return md5_checksums_by_library_id
    


def check_ready_to_upload(config, run_dir):
    """
    Check if a run dir is ready to upload.

    :param config: Application config
    :type config: dict
    :param run_dir: Path to run directory to be uploaded
    :type run_dir: str
    """
    upload_prepared_path = os.path.join(run_dir, 'upload_prepared.json')
    samplelist_path = os.path.join(run_dir, 'SampleList.csv')
    if not os.path.exists(upload_prepared_path):
        return False
    if not os.path.exists(samplelist_path):
        return False

    samplelist_data_by_library_id = parse_samplelist(samplelist_path)
    upload_preparation_data = {}
    try:
        with open(upload_prepared_path, 'r') as f:
            upload_preparation_data = json.load(f)
        # print(json.dumps(upload_preparation_data, indent=2))
        # exit()
    except json.JSONDecodeError as e:
        logging.error(json.dumps({
            "event_type": "parse_upload_prepared_failed",
            "upload_prepared_file_path": upload_prepared_path,
        }))
        return False

    md5_checksums_by_library_id = collect_md5_checksums(run_dir, samplelist_data_by_library_id)

    md5_checksums_match_by_library_id = {}
    for library in upload_preparation_data.get('libraries', []):
        library_id = library['library_id']
        expected_fastq_forward_md5 = library['fastq_forward_md5']
        expected_fastq_reverse_md5 = library['fastq_reverse_md5']
        collected_fastq_forward_md5 = md5_checksums_by_library_id[library_id]['fastq_forward_md5']
        collected_fastq_reverse_md5 = md5_checksums_by_library_id[library_id]['fastq_reverse_md5']
        checksums_match = (collected_fastq_forward_md5 == expected_fastq_forward_md5) and (collected_fastq_reverse_md5 == expected_fastq_reverse_md5)
        md5_checksums_match_by_library_id[library_id] = checksums_match

    if all(md5_checksums_match_by_library_id.values()):
        return True
    else:
        return False


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
            if not os.path.isdir(timestamped_subdir):
                continue
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
                ready_to_upload = check_ready_to_upload(config, os.path.abspath(subdir))

                not_already_uploaded = True
                irida_uploader_status_path = os.path.join(subdir.path, 'irida_upload_completed.json')
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
                    logging.info(json.dumps({"event_type": "directory_skipped", "run_directory_path": os.path.abspath(subdir.path), "conditions_checked": conditions_checked}))
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
            # Commented out 2023-11-06 by dfornika <dan.fornika@bccdc.ca>
            # for troubleshooting multiple-upload issue
            # shutil.rmtree(upload_parent_dir)
            # logging.info(json.dumps({"event_type": "directory_deleted", "directory_path": upload_parent_dir}))
            irida_upload_completed_path = os.path.join(upload_dir, 'irida_upload_completed.json')
            irida_upload_completed_contents = {}
            with open(irida_upload_completed_path, 'w') as f:
                json.dump(irida_upload_completed_contents, f, indent=2)
                f.write('\n')
