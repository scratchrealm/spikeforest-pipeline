import click
import json
import runarepo
from typing import List
import kachery_client as kc
from Job import Job

def _run_compare_with_truth(sorting_npz_uri: str, sorting_true_npz_uri: str) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        sorting_npz_path = kc.load_file(sorting_npz_uri)
        assert sorting_npz_path is not None, f'Unable to load: {sorting_npz_uri}'
        sorting_true_npz_path = kc.load_file(sorting_true_npz_uri)
        assert sorting_true_npz_path is not None, f'Unable to load: {sorting_true_npz_uri}'
        output_dir = f'{tmpdir}/output'

        repo = 'https://github.com/scratchrealm/spikesorting-runarepo'
        subpath = 'compare-with-truth'

        print(f'Running {repo} {subpath}')
        inputs = [
            runarepo.Input(name='INPUT_SORTING_NPZ', path=sorting_npz_path),
            runarepo.Input(name='INPUT_SORTING_TRUE_NPZ', path=sorting_true_npz_path)
        ]
        runarepo.run(repo, subpath=subpath, inputs=inputs, output_dir=output_dir, use_docker=False)

        print('Loading comparison output...')
        with open(f'{output_dir}/comparison.json') as f:
            comparison = json.load(f)
        return comparison

@click.command()
@click.argument('config_name')
def main(config_name: str):
    jobs0 = kc.get({'type': 'sfworkflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'compare-with-truth']
    jobs_to_run = [
        job for job in jobs
        if job.force_run or (kc.get(job.key()) is None)
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
        output = _run_compare_with_truth(**job.kwargs)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()
