import click
import json
import runarepo
from typing import List
import sortingview as sv
import kachery_client as kc
from Job import Job
from spikeinterface.core.old_api_utils import OldToNewRecording
from nwb_conversion_tools.utils.spike_interface import write_recording
import spikeinterface.extractors as se

def _run_prepare_recording_nwb_job(recording_uri: str) -> dict:
    with kc.TemporaryDirectory() as tmpdir:
        recording_nwb_path = f'{tmpdir}/recording.nwb'

        print('Loading recording...')
        recording_object = kc.load_json(recording_uri)
        assert recording_object is not None, f'Unable to load recording: {recording_uri}'
        recording = sv.LabboxEphysRecordingExtractor(recording_object)
        recording = OldToNewRecording(recording)
        recording.clear_channel_groups()
        print('Writing recording nwb...')
        write_recording(recording, save_path=recording_nwb_path, compression=None, compression_opts=None)
        print('Storing recording nwb...')
        recording_nwb_uri = kc.store_file(recording_nwb_path)
        return {'recording_nwb_uri': recording_nwb_uri}

@click.command()
@click.argument('config_name')
def main(config_name: str):
    jobs0 = kc.get({'type': 'sfworkflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'prepare-recording-nwb']
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
        output = _run_prepare_recording_nwb_job(**job.kwargs)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()