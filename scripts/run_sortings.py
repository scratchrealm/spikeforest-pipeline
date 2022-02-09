#!/usr/bin/env python3

import os
import click
import yaml
import kachery_client as kc
from typing import Dict, List, Union
from Job import Job
import sorting

def _process_settings_config(settings_file: str) -> Dict[str, any]:
    try:
        with open(settings_file, 'r') as f:
            settings = yaml.safe_load(f)
            return _make_valid_settings(settings)
    except (OSError, IOError):
        return _make_valid_settings({})

def _make_valid_settings(settings: Dict) -> Dict:
    _ensure_defaults(settings)
    _validate_containerization(settings)
    _validate_worker_count(settings)
    _validate_slurm_config(settings)
    return settings

def _ensure_defaults(settings: Dict) -> None:
    default_settings = {
        'force_run': False,
        'docker': False,
        'singularity': False,
        'images': None, # or a path to a yaml file associating algorithms with their container files
        'slurm_config': None,
        'max_simultaneous_sorts': 1
    }
    for key in default_settings.keys():
        if key in settings: continue
        settings[key] = default_settings[key]
    # Not sure if we'll use this flag; if we do, it'll get reset when validating slurm config
    settings['use_slurm'] = False

def _validate_containerization(settings: Dict) -> None:
    # Ensure exactly 0-1 of 'docker' and 'singularity' have been selected.
    if (settings['docker'] and settings['singularity']):
        print(f"Settings requested both docker and singularity, but they cannot both be run. Disabling containerization.")
        settings['docker'] = False
        settings['singularity'] = False
        settings['images'] = None # don't bother parsing the container images if we aren't using containerization
    if not (settings['docker'] or settings['singularity']):
        # don't bother parsing container-images file if containerization is not requested
        settings['images'] = None
    if not settings['images']: return
    # If containerization has validly been requested, validate the images file.
    try:
        with open(settings['images'], 'r') as f:
            settings['images'] = yaml.safe_load(f)
            # TODO: Check that the specified container files can be found?
    except (OSError, IOError):
        print(f'Unable to load image dictionary {settings["images"]}. Disabling containerization.')
        settings['images'] = None
        settings['docker'] = False
        settings['singularity'] = False

def _validate_worker_count(settings: Dict) -> None:
    try:
        workers = int(settings['max_simultaneous_sorts'])
        if (workers < 1):
            raise ValueError
    except ValueError:
        print(f'Value {settings["max_simultaneous_sorts"]} is not a positive integer; setting max simultaneous sorts to 1.')
        workers = 1
    settings['max_simultaneous_sorts'] = workers

def _validate_slurm_config(settings: Dict) -> None:
    if not (settings['slurm_config']): return
    ## TODO
    settings['slurm_config'] = {}
    settings['use_slurm'] = True


@click.command()
@click.argument('config_file')
@click.argument('processing_settings')
def main(config_file: str, processing_settings: str):
    settings = _process_settings_config(processing_settings)
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']

    # Fetch jobs and hydrate as Job objects, then filer to sorting-type jobs that need to be run
    job_dicts = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job_dict) for job_dict in job_dicts]
    jobs = [job for job in jobs if job.type == 'sorting']
    if (not settings['force_run']):
        jobs = [job for job in jobs if job.force_run or (kc.get(job.key()) is None)]

    if(settings['use_slurm']):
        # TODO
        # create a command line and config file for each slurm batch
        # need to write and call appropriate slurm batch file
        jobs_cnt = len(jobs)
        ### TODO: Actually count jobs per algorithm or something, there may be variation?
        # Okay. So we would rather put more jobs in the slurm batch file
        # than fewer, because we want to get scheduled, which means we want the jobs
        # to take a shorter not a longer amount of time.
        # Basically, write a slurm job array. Allocate min one worker for each algo,
        # and cap each worker at 3x the max simultaneous workers. (This is arbitrary.)
        # So need to write mock config files for each of them.
        # (Assume that the workers will have the local directory mounted in.)
        # Then write a local bash file with the SBATCH settings that calls the
        # run_sortings.py script to actually Do The Thing.
        pass
    else:
        # Local run--we'll run one instance of sorting.py per sorter; instances in serial.
        images = settings['images'] or {}
        for sorter in config['sorters']:
            image = images.get(sorter)
            docker = settings['docker'] and image
            singularity = settings['singularity'] and image
            sorting.main(config_file,
                sorter,
                settings['max_simultaneous_sorts'],
                settings['force_run'],
                docker,
                singularity,
                image
            )

if __name__ == '__main__':
    main()