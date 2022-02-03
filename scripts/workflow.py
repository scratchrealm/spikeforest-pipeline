import click
import yaml
from typing import Union
import kachery_client as kc
from Job import Job

sorters = [
    {
        'algorithm': 'mountainsort4',
        'sorting_params': {
            "detect_sign": -1,
            "adjacency_radius": 50,
            "freq_min": 300,
            "freq_max": 6000,
            "filter": True,
            "whiten": True,
            "num_workers": 1,
            "clip_size": 50,
            "detect_threshold": 3,
            "detect_interval": 10
        }
    }
]

@click.command()
@click.argument('config_file')
def main(config_file: str):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    jobs = []
    results = []
    study1 = kc.load_json('sha1://7b85423dc46f34ebd7b12559d6d2131de1eb7473/paired_kampff.json')
    recordings = study1['recordings'][:1]
    for recording in recordings:
        recording_label = f'{recording["studyName"]}/{recording["name"]}'
        job = Job(
            type='prepare-recording-nwb',
            label=f'Prepare recording nwb: {recording_label}',
            kwargs={
                'recording_uri': recording['recordingUri']
            },
            force_run=False
        )
        jobs.append(job)
        output = kc.get(job.key())

        job_prepare_sorting_true = Job(
            type='prepare-sorting-true-npz',
            label=f'Prepare sorting true npz: {recording_label}',
            kwargs={
                'sorting_true_uri': recording['sortingTrueUri']
            },
            force_run=False
        )
        jobs.append(job_prepare_sorting_true)
        output_prepare_sorting_true = kc.get(job_prepare_sorting_true.key())
        if output_prepare_sorting_true is not None and kc.load_file(output_prepare_sorting_true.get('sorting_true_npz_uri', '')):
            sorting_true_npz_uri = output_prepare_sorting_true['sorting_true_npz_uri']
        else:
            sorting_true_npz_uri = None

        if output is not None and kc.load_file(output.get('recording_nwb_uri', '')) is not None:
            recording_nwb_uri = output['recording_nwb_uri']
            for sorter in sorters:
                algname = sorter['algorithm']
                sorting_params = sorter['sorting_params']
                job = Job(
                    type='sorting',
                    label=f'{algname} {recording_label}',
                    kwargs={
                        'algorithm': algname,
                        'recording_nwb_uri': recording_nwb_uri,
                        'sorting_params': sorting_params
                    },
                    force_run=False
                )
                jobs.append(job)
                output = kc.get(job.key())
                if output is not None and kc.load_file(output.get('sorting_npz_uri', '')) is not None:
                    sorting_npz_uri = output['sorting_npz_uri']
                    if sorting_true_npz_uri is not None:
                        job = Job(
                            type='compare-with-truth',
                            label=f'compare with truth {algname} {recording_label}',
                            kwargs={
                                'sorting_npz_uri': sorting_npz_uri,
                                'sorting_true_npz_uri': sorting_true_npz_uri
                            },
                            force_run=False
                        )
                        jobs.append(job)
                        output = kc.get(job.key())
                        if output is not None:
                            results.append({
                                'recording': recording,
                                'sorter': sorter,
                                'sorting_npz_uri': sorting_npz_uri,
                                'sorting_true_npz_uri': sorting_true_npz_uri,
                                'comparison_with_truth': output
                            })
    print('-----------------------------')
    for job in jobs:
        print(job.type)
    print('-----------------------------')
    print(results)
    kc.set({'type': 'sfworkflow-jobs', 'name': config['name']}, [job.to_dict() for job in jobs])
    kc.set({'type': 'sfworkflow-results', 'name': config['name']}, results)

if __name__ == '__main__':
    main()