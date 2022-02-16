#!/usr/bin/env python3

import os
from builtins import bool
import click
import yaml
import json
from random import shuffle
import runarepo
from typing import List, Union
import kachery_client as kc
from Job import Job
from multiprocessing import Pool
from functools import partial

subpaths = {
    'mountainsort4': 'mountainsort4',
    'spykingcircus': 'spykingcircus',
    'tridesclous': 'tridesclous',
    'kilosort3': 'kilosort3',
    'kilosort2_5': 'kilosort2_5',
    'kilosort2': 'kilosort2',
}

def _run_sorting_job(algorithm: str, recording_nwb_uri: str, sorting_params: dict, use_docker: bool=False, use_singularity: bool=False, image: Union[str, None]=None) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        sorting_params_path = f'{tmpdir}/sorting_params.json'
        recording_nwb_path = kc.load_file(recording_nwb_uri)
        assert recording_nwb_path is not None, f'Unable to load recording nwb: {recording_nwb_uri}'
        output_dir = f'{tmpdir}/output'

        repo = os.environ.get('SPIKESORTING_RUNAREPO_PATH', 'https://github.com/scratchrealm/spikesorting-runarepo')
        subpath = subpaths.get(algorithm)
        assert subpath is not None, f'Unsupported algorithm: {algorithm}'

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

def _run_sorting_jobs_wrapper(job: Job, config_name: str, verbose: bool, dry_run: bool, **kwargs):
    job_key = _get_job_key(job.label, config_name)
    got_mutex = kc.set(job_key, os.getpid(), update=False)
    if not got_mutex:
        # unable to acquire mutex: someone else must have claimed this job, so we can skip it
        if (verbose): print(f"\tUnable to get lock {job_key}, skipping.")
        return
    if (verbose): print(f"\tGot lock for job {job_key}")
    # try:
    print(f'Running: {job.label}')
    if (not dry_run):
        output = _run_sorting_job(**job.kwargs, **kwargs)
        kc.set(job.key(), output)
    else:
        output = "DRY RUN: JOB SKIPPED"
    print(f'OUTPUT of {job.label}:\n{output}')
    # Actually we don't want to do this--it can result in re-running jobs.
    # finally:
    #     kc.delete(job_key) # Release the mutex (needs to happen even if something failed)
    #     if (verbose): print(f"\tReleasing lock {job_key}")
    #     # NOTE POSSIBILITY FOR INFINITE LOOP if running with rerun-failing

def _get_job_key(label: str, config_name: str):
    return f"{config_name}-running-sorting-{label}"

def _reset_locks(jobs: List[Job], config_name: str):
    locks_reset = 0
    for job in jobs:
        job_key = _get_job_key(job.label, config_name)
        if (kc.get(job_key) is not None):
            locks_reset += 1
        kc.delete(job_key)
    return locks_reset

def _init_config(config_file: str, docker: bool, singularity: bool, num_parallel: Union[str, None]=None):
    if docker and singularity:
        raise Exception('Both singularity and docker were requested, but no more than one can be used simultaneously.')
    if num_parallel is None:
        num_parallel = 1
    else:
        num_parallel = max(1, int(num_parallel))
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    return (config_name, docker, singularity, num_parallel)

def _get_jobs_list(config_name: str, algorithm: str):
    jobs_dict = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job_dict) for job_dict in jobs_dict]
    #### TODO: Should this 'algorithm' actually be 'name'?
    jobs = [job for job in jobs if job.type == 'sorting' and job.kwargs['algorithm'] == algorithm]
    return jobs

def _filter_jobs_to_run(all_jobs: List[Job], force_run: bool, rerun_failing: bool):
    if force_run:
        return all_jobs

    jobs: List[Job] = []
    for job in all_jobs:
        key = kc.get(job.key())
        if job.force_run or (key is None) or (key['sorting_npz_uri'] is None and rerun_failing):
            jobs.append(job)
    return jobs

def _describe_jobs_to_run(jobs: List[Job], num_parallel: int):
    print('JOBS TO RUN:')
    for job in jobs:
        print(job.label)
    print('')
    print(f'Total number of jobs: {len(jobs)}')
    print(f'Number of jobs to run: {len(jobs)}')
    print(f'Number of jobs run simultaneously: {num_parallel}')


@click.command()
@click.argument('config_file')
@click.argument('algorithm')
@click.option('--reset-locks', is_flag=True, help="Clear out all locks on sorting jobs for this algorithm")
@click.option('--num-parallel', help="Maximum number of sorting jobs to run simultaneously")
@click.option('--force-run', is_flag=True, help="Force rerurn")
@click.option('--rerun-failing', is_flag=True, help="Rerun the failing jobs")
@click.option('--docker', is_flag=True, help="Use docker image")
@click.option('--singularity', is_flag=True, help="Use singularity image")
@click.option('--image', default=None, help='Image for use in docker or singularity mode')
@click.option('--use-deterministic-job-order', is_flag=True, help="If unset, will skip shuffling the order of jobs")
@click.option('--dry-run', is_flag=True, help="If set, sorters won't actually be called.")
@click.option('--verbose', is_flag=True, help="Detailed output about steps taken")
def main(
    config_file: str,
    algorithm: str,
    reset_locks: bool,
    num_parallel: Union[int, None],
    force_run: bool,
    rerun_failing: bool,
    docker: bool,
    singularity: bool,
    image: Union[str, None],
    use_deterministic_job_order: bool,
    dry_run: bool,
    verbose: bool
):
    (config_name, docker, singularity, num_parallel) = _init_config(config_file, docker, singularity, num_parallel)
    all_matched_jobs = _get_jobs_list(config_name, algorithm)
    if (reset_locks):
        if verbose: print(f"Resetting locks for {config_file} algorithnm {algorithm}")
        locks_reset = _reset_locks(all_matched_jobs, config_name)
        if verbose: print(f"{locks_reset} locks reset.")
        return

    jobs_to_run = _filter_jobs_to_run(all_matched_jobs, force_run, rerun_failing)
    if(len(jobs_to_run) > 0 and not use_deterministic_job_order):
        shuffle(jobs_to_run)
    _describe_jobs_to_run(jobs_to_run, num_parallel)

    # Curry the command line parameters so we can just pass the Job object later on.
    run_sorting_job_partial = partial(_run_sorting_jobs_wrapper, use_docker=docker, use_singularity=singularity, image=image, config_name=config_name, dry_run=dry_run, verbose=verbose)

    if (num_parallel == 1):
        for job in jobs_to_run:
            run_sorting_job_partial(job)
    else:
        pool = Pool(num_parallel)
        list(pool.imap(run_sorting_job_partial, jobs_to_run, chunksize=max(1, len(jobs_to_run)//num_parallel)))
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()