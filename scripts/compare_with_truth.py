#!/usr/bin/env python3

import os
import click
import yaml
import runarepo
from typing import List, Union
import kachery_client as kc
from Job import Job

def _run_compare_with_truth(sorting_npz_uri: str, sorting_true_npz_uri: str, use_docker: bool, use_singularity: bool, image: Union[str, None]) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        sorting_npz_path = kc.load_file(sorting_npz_uri)
        assert sorting_npz_path is not None, f'Unable to load: {sorting_npz_uri}'
        sorting_true_npz_path = kc.load_file(sorting_true_npz_uri)
        assert sorting_true_npz_path is not None, f'Unable to load: {sorting_true_npz_uri}'
        output_dir = f'{tmpdir}/output'

        repo = os.environ.get('SPIKESORTING_RUNAREPO_PATH', 'https://github.com/scratchrealm/spikesorting-runarepo')
        subpath = 'compare-with-truth'

        print(f'Running {repo} {subpath}')
        inputs = [
            runarepo.Input(name='INPUT_SORTING_NPZ', path=sorting_npz_path),
            runarepo.Input(name='INPUT_SORTING_TRUE_NPZ', path=sorting_true_npz_path)
        ]
        output = runarepo.run(repo, subpath=subpath, inputs=inputs, output_dir=output_dir, use_docker=use_docker, use_singularity=use_singularity, image=image)
        if output.retcode != 0:
            raise Exception(f'Non-zero return code in comparison: {output.retcode}')

        print('Storing comparison output...')
        comparison_uri = kc.store_file(f'{output_dir}/comparison.json')
        return {'comparison_uri': comparison_uri}

@click.command()
@click.argument('config_file')
@click.option('--docker', is_flag=True, help="Use docker images")
@click.option('--force-run', is_flag=True, help="Force rerurn")
@click.option('--singularity', is_flag=True, help="Use singularity image")
@click.option('--image', default=None, help='Image for use in docker or singularity mode')
def main(config_file: str, docker: bool, force_run: bool, singularity: bool, image: Union[str, None]):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    jobs0 = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'compare-with-truth']
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
    print('')

    for job in jobs_to_run:
        print(f'Running: {job.label}')
        output = _run_compare_with_truth(**job.kwargs, use_docker=docker, use_singularity=singularity, image=image)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()
