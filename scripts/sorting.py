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
from multiprocessing import Pool

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
        elif algorithm == 'tridesclous':
            subpath = 'tridesclous'
        elif algorithm == 'kilosort3':
            subpath = 'kilosort3'
        elif algorithm == 'kilosort2_5':
            subpath = 'kilosort2_5'
        elif algorithm == 'kilosort2':
            subpath = 'kilosort2'
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
        output = runarepo.run(repo, subpath=subpath, inputs=inputs, output_dir=output_dir, use_docker=use_docker, use_singularity=use_singularity, image=image)
        print(f'Storing console ouput')
        console_lines_uri = kc.store_json(output.console_lines)
        if output.retcode == 0:
            print('Storing sorting output...')
            sorting_npz_path = f'{output_dir}/sorting.npz'
            sorting_npz_uri = kc.store_file(sorting_npz_path)
        else:
            print(f'Nonzero exit code for sorting run: {output.retcode}')
            sorting_npz_uri = None
        
        return {
            'retcode': output.retcode,
            'console_lines_uri': console_lines_uri,
            'sorting_npz_uri': sorting_npz_uri
        }

@click.command()
@click.argument('config_file')
@click.argument('algorithm')
@click.option('--max-simultaneous-sorts', is_flag=True, help="Maximum number of sorting jobs to run simultaneously")
@click.option('--force-run', is_flag=True, help="Force rerurn")
@click.option('--rerun-failing', is_flag=True, help="Rerun the failing jobs")
@click.option('--docker', is_flag=True, help="Use docker image")
@click.option('--singularity', is_flag=True, help="Use singularity image")
@click.option('--image', default=None, help='Image for use in docker or singularity mode')
def main(config_file: str, algorithm: str, max_simultaneous_sorts: Union[int, None], force_run: bool, rerun_failing: bool, docker: bool, singularity: bool, image: Union[str, None]):
    if docker and singularity:
        raise Exception('Both singularity and docker were requested, but no more than one can be used simultaneously.')
    if max_simultaneous_sorts is None or max_simultaneous_sorts < 1:
        max_simultaneous_sorts = 1
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    jobs0 = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    #### TODO: Should this 'algorithm' actually be 'name'?
    jobs = [job for job in jobs if job.type == 'sorting' and job.kwargs['algorithm'] == algorithm]
    jobs_to_run: List[Job] = []
    for job in jobs:
        a = kc.get(job.key())
        if force_run or job.force_run or (a is None) or (a['sorting_npz_uri'] is None and rerun_failing):
            jobs_to_run.append(job)
    print('JOBS TO RUN:')
    for job in jobs_to_run:
        print(job.label)
    print('')
    print(f'Total number of jobs: {len(jobs)}')
    print(f'Number of jobs to run: {len(jobs_to_run)}')
    print(f'Number of jobs run simultaneously: {max_simultaneous_sorts}')

    def _run_sorting_job_wrapper(job: Job) -> None:
        print(f'Running: {job.label}')
        output = _run_sorting_job(**job.kwargs, use_docker=docker, use_singularity=singularity, image=image)
        print(f"OUTPUT of {job.label}:\n{output}")
        kc.set(job.key(), output)

    if (max_simultaneous_sorts == 1):
        for job in jobs_to_run:
            _run_sorting_job_wrapper(job)
    else:
        pool = Pool(max_simultaneous_sorts)
        list(pool.imap(_run_sorting_job_wrapper, jobs_to_run, chunksize=len(jobs_to_run)//max_simultaneous_sorts))
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()