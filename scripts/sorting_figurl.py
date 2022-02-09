#!/usr/bin/env python3

import os
import click
import yaml
from typing import List, Union
import kachery_client as kc
from Job import Job
from spikeinterface import extractors as se
from spikeinterface.core.old_api_utils import NewToOldSorting
import sortingview as sv
from sortingview.SpikeSortingView import SpikeSortingView, create_console_view

def _run_sorting_figurl(recording_nwb_uri: str, sorting_npz_uri: str, label: str, sorting_console_lines_uri: Union[str, None]=None) -> dict:
    recording_nwb = kc.load_file(recording_nwb_uri)
    assert recording_nwb is not None, f'Unable to load file: {recording_nwb_uri}'
    sorting_npz = kc.load_file(sorting_npz_uri)
    assert sorting_npz is not None, f'Unable to load file: {sorting_npz_uri}'
    if sorting_console_lines_uri is not None:
        sorting_console_lines = kc.load_json(sorting_console_lines_uri)
        if sorting_console_lines is None: f'Warning: Unable to load sorting console: {sorting_console_lines_uri}'
    else:
        sorting_console_lines = None
    
    recording = sv.LabboxEphysRecordingExtractor({
        'recording_format': 'nwb',
        'data': {
            'path': recording_nwb
        }
    })
    sorting = NewToOldSorting(se.NpzSortingExtractor(sorting_npz))
    sorting = sv.LabboxEphysSortingExtractor.from_memory(sorting=sorting, serialize=True)

    print('Preparing spikesortingview data')
    X = SpikeSortingView.create(
        recording=recording,
        sorting=sorting,
        segment_duration_sec=60 * 20,
        snippet_len=(20, 20),
        max_num_snippets_per_segment=100,
        channel_neighborhood_size=7
    )

    print('Preparing summary')
    f1 = X.create_summary()
    print('Preparing units table')
    f2 = X.create_units_table(unit_ids=X.unit_ids)
    print('Preparing autocorrelograms')
    f3 = X.create_autocorrelograms(unit_ids=X.unit_ids)
    print('Preparing raster plot')
    f4 = X.create_raster_plot(unit_ids=X.unit_ids)
    print('Preparing average waveforms')
    f5 = X.create_average_waveforms(unit_ids=X.unit_ids)
    print('Preparing spike amplitudes')
    f6 = X.create_spike_amplitudes(unit_ids=X.unit_ids)
    print('Preparing electrode geometry')
    f7 = X.create_electrode_geometry()
    # f8 = X.create_live_cross_correlograms()

    figures = [f1, f2, f3, f4, f5, f6, f7]
    if sorting_console_lines is not None:
        print('Preparing console view')
        figures.append(
            create_console_view(console_lines=sorting_console_lines)
        )

    mountain_layout = X.create_mountain_layout(figures=figures, label=label)

    url = mountain_layout.url()
    return {'sorting_figurl': url}

@click.command()
@click.argument('config_file')
@click.option('--force-run', is_flag=True, help="Force rerurn")
def main(config_file: str, force_run: bool):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    jobs0 = kc.get({'type': 'spikeforest-workflow-jobs', 'name': config_name})
    jobs: List[Job] = [Job.from_dict(job0) for job0 in jobs0]
    jobs = [job for job in jobs if job.type == 'sorting-figurl']
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
        output = _run_sorting_figurl(**job.kwargs)
        print('OUTPUT')
        print(output)
        kc.set(job.key(), output)

if __name__ == '__main__':
    main()
