import click
import json
import runarepo
from typing import List
import sortingview as sv
import kachery_client as kc
from Job import Job
from spikeinterface.core.old_api_utils import OldToNewSorting
import spikeinterface.extractors as se

def _run_prepare_sorting_true_npz_job(sorting_true_uri: str) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        sorting_true_npz_path = f'{tmpdir}/sorting_true.npz'

        print('Loading sorting true...')
        sorting_object = kc.load_json(sorting_true_uri)
        assert sorting_object is not None, f'Unable to load sorting: {sorting_true_uri}'
        sorting = sv.LabboxEphysSortingExtractor(sorting_object)
        sorting = OldToNewSorting(sorting)
        print('Writing sorting true npz...')
        se.NpzSortingExtractor.write_sorting(sorting=sorting, save_path=sorting_true_npz_path)
        print('Storing sorting true npz...')
        sorting_true_npz_uri = kc.store_file(sorting_true_npz_path)
        return {'sorting_true_npz_uri': sorting_true_npz_uri}

@click.command()
@click.argument('config_name')
def main(config_name: str):
    jobs0 = kc.get({'type': 'sfworkflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'prepare-sorting-true-npz']
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
        output = _run_prepare_sorting_true_npz_job(**job.kwargs)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()