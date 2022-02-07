#!/usr/bin/env python3

import os
from builtins import bool
import click
import yaml
import json
import runarepo
from typing import List, Union
import kachery_client as kc
from Job import Job

def _run_sorting_job(algorithm: str, recording_nwb_uri: str, sorting_params: dict, use_docker: bool=False, use_singularity: bool=False, image: Union[str, None]=None) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        sorting_params_path = f'{tmpdir}/sorting_params.json'
        recording_nwb_path = kc.load_file(recording_nwb_uri)
        assert recording_nwb_path is not None, f'Unable to load recording nwb: {recording_nwb_uri}'
        output_dir = f'{tmpdir}/output'

        repo = os.environ.get('SPIKESORTING_RUNAREPO_PATH', 'https://github.com/scratchrealm/spikesorting-runarepo')
        if algorithm == 'mountainsort4':
            subpath = 'mountainsort4'
        elif algorithm == 'spykingcircus':
            subpath = 'spykingcircus'
        else:
            raise Exception(f'Unexpected algorithm: {algorithm}')

        print('Writing sorting params...')
        with open(sorting_params_path, 'w') as f:
            json.dump(sorting_params, f)

        print(f'Running {repo} {subpath}')
        inputs = [
            runarepo.Input(name='INPUT_RECORDING_NWB', path=recording_nwb_path),
            runarepo.Input(name='INPUT_SORTING_PARAMS', path=sorting_params_path)
        ]
        runarepo.run(repo, subpath=subpath, inputs=inputs, output_dir=output_dir, use_docker=use_docker, use_singularity=use_singularity, image=image)
        print('Storing sorting output...')
        sorting_npz_path = f'{output_dir}/sorting.npz'
        sorting_npz_uri = kc.store_file(sorting_npz_path)
        
        return {'sorting_npz_uri': sorting_npz_uri}

@click.command()
@click.argument('config_file')
@click.argument('algorithm')
@click.option('--force-run', is_flag=True, help="Force rerurn")
@click.option('--docker', is_flag=True, help="Use docker image")
@click.option('--singularity', is_flag=True, help="Use singularity image")
@click.option('--image', default=None, help='Image for use in docker or singularity mode')
def main(config_file: str, algorithm: str, force_run: bool, docker: bool, singularity: bool, image: Union[str, None]):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    jobs0 = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'sorting' and job.kwargs['algorithm'] == algorithm]
    jobs_to_run = [
        job for job in jobs
        if force_run or job.force_run or (kc.get(job.key()) is None)
    ]
    print('JOBS TO RUN:')
    for job in jobs_to_run:
        print(job.label)
    print('')
    print(f'Total number of jobs: {len(jobs)}')
    print(f'Number of jobs to run: {len(jobs_to_run)}')

    for job in jobs_to_run:
        print(f'Running: {job.label}')
        output = _run_sorting_job(**job.kwargs, use_docker=docker, use_singularity=singularity, image=image)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()