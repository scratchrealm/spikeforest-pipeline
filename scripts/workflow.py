import click
import yaml
from typing import List, Union
import kachery_client as kc
from Job import Job


class Workflow:
    def __init__(self) -> None:
        self._jobs: List[Job] = []
        self._results: List[dict] = []
    def add_job(self, job: Job):
        self._jobs.append(job)
    def add_result(self, result: dict):
        self._results.append(result)
    @property
    def jobs(self):
        return self._jobs.copy()
    @property
    def results(self):
        return self._results.copy()

@click.command()
@click.argument('config_file')
def main(config_file: str):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    config_sorters = config['sorters']
    config_studies = config['studies']

    sf_study_sets_uri = 'sha1://f728d5bf1118a8c6e2dfee7c99efb0256246d1d3/studysets.json'
    sf_study_sets = kc.load_json(sf_study_sets_uri)
    assert sf_study_sets is not None, f'Unable to load sf study sets: {sf_study_sets_uri}'

    workflow = Workflow()
    
    # study1 = kc.load_json('sha1://7b85423dc46f34ebd7b12559d6d2131de1eb7473/paired_kampff.json')
    # recordings = study1['recordings'][:1]

    for config_study in config_studies:
        config_sorters = [[s for s in config_sorters if s['name'] == sorter_name][0] for sorter_name in config_study['sorter_names']]
        for recording_name in config_study['recording_names']:
            recording = _get_spikeforest_recording(sf_study_sets, config_study['study_set_name'], config_study['study_name'], recording_name)
            recording_nwb_uri = _prepare_recording_nwb(workflow, recording)
            sorting_true_npz_uri = _prepare_sorting_true_npz(workflow, recording)
            for sorter in config_sorters:
                sorting_npz_uri = _sorting(workflow, recording, recording_nwb_uri, sorter)
                comparison_uri = _compare_with_truth(workflow, recording, sorter, sorting_npz_uri, sorting_true_npz_uri)
                if sorting_npz_uri is not None and comparison_uri is not None and kc.load_file(comparison_uri, local_only=True) is not None:
                    workflow.add_result({
                        'recording': recording,
                        'sorter': sorter,
                        'recording_nwb_uri': recording_nwb_uri,
                        'sorting_true_npz_uri': sorting_true_npz_uri,
                        'sorting_npz_uri': sorting_npz_uri,
                        'comparison_with_truth': kc.load_json(comparison_uri)
                    })
    kc.set({'type': 'spikeforest-workflow-jobs', 'name': config_name}, [job.to_dict() for job in workflow.jobs])
    kc.set({'type': 'spikeforest-workflow-results', 'name': config_name}, workflow.results)
    print('-----------------------------')
    for job in workflow.jobs:
        print(job.type)
    print('-----------------------------')
    print(workflow.results)

def _prepare_recording_nwb(workflow: Workflow, recording: dict):
    recording_label = f'{recording["studyName"]}/{recording["name"]}'
    job = Job(
        type='prepare-recording-nwb',
        label=f'Prepare recording nwb: {recording_label}',
        kwargs={
            'recording_uri': recording['recordingUri']
        },
        force_run=False
    )
    workflow.add_job(job)
    output = kc.get(job.key())
    recording_nwb_uri = output.get('recording_nwb_uri', None) if output is not None else None
    recording_nwb_uri = recording_nwb_uri if recording_nwb_uri and kc.load_file(recording_nwb_uri, local_only=True) is not None else None
    return recording_nwb_uri

def _prepare_sorting_true_npz(workflow: Workflow, recording: dict):
    recording_label = f'{recording["studyName"]}/{recording["name"]}'
    job = Job(
        type='prepare-sorting-true-npz',
        label=f'Prepare sorting true npz: {recording_label}',
        kwargs={
            'sorting_true_uri': recording['sortingTrueUri']
        },
        force_run=False
    )
    workflow.add_job(job)
    output = kc.get(job.key())
    sorting_true_npz_uri = output.get('sorting_true_npz_uri', None) if output is not None else None
    sorting_true_npz_uri = sorting_true_npz_uri if sorting_true_npz_uri and kc.load_file(sorting_true_npz_uri, local_only=True) is not None else None
    return sorting_true_npz_uri

def _sorting(workflow: Workflow, recording: dict, recording_nwb_uri: Union[str, None], sorter: dict):
    if recording_nwb_uri is None: return None
    recording_label = f'{recording["studyName"]}/{recording["name"]}'
    sorter_name = sorter['name']
    algname = sorter['algorithm']
    sorting_params = sorter['sorting_params']
    job = Job(
        type='sorting',
        label=f'{sorter_name} {recording_label}',
        kwargs={
            'algorithm': algname,
            'recording_nwb_uri': recording_nwb_uri,
            'sorting_params': sorting_params
        },
        force_run=False
    )
    workflow.add_job(job)
    output = kc.get(job.key())
    sorting_npz_uri = output.get('sorting_npz_uri', None) if output is not None else None
    sorting_npz_uri = sorting_npz_uri if sorting_npz_uri and kc.load_file(sorting_npz_uri, local_only=True) is not None else None
    return sorting_npz_uri

def _compare_with_truth(workflow: Workflow, recording: dict, sorter: dict, sorting_npz_uri: Union[str, None], sorting_true_npz_uri: Union[str, None]):
    if sorting_npz_uri is None: return None
    if sorting_true_npz_uri is None: return None
    recording_label = f'{recording["studyName"]}/{recording["name"]}'
    sorter_name = sorter['name']
    job = Job(
        type='compare-with-truth',
        label=f'compare with truth {sorter_name} {recording_label}',
        kwargs={
            'sorting_npz_uri': sorting_npz_uri,
            'sorting_true_npz_uri': sorting_true_npz_uri
        },
        force_run=False
    )
    workflow.add_job(job)
    output = kc.get(job.key())
    comparison_uri = output.get('comparison_uri', None) if output is not None else None
    comparison_uri = comparison_uri if comparison_uri and kc.load_file(comparison_uri, local_only=True) is not None else None
    return comparison_uri

def _get_spikeforest_recording(sf_study_sets: dict, study_set_name: str, study_name: str, recording_name: str):
    try:
        study_set = [s for s in sf_study_sets['StudySets'] if s['name'] == study_set_name][0]
        study = [s for s in study_set['studies'] if s['name'] == study_name][0]
        recording = [r for r in study['recordings'] if r['name'] == recording_name][0]
    except:
        raise Exception(f'Unable to find spikeforest recording: {study_set_name} {study_name} {recording_name}')
    return recording

if __name__ == '__main__':
    main()