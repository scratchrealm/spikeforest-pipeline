#!/usr/bin/env python3

import click
import json
import yaml
import kachery_client as kc

@click.command()
@click.argument('config_file')
@click.option('--json-format', is_flag=True, help='Dump all results to JSON')
def main(config_file: str, json_format: str):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    print(f'Config name: {config_name}')
    results = kc.get({'type': 'spikeforest-workflow-results', 'name': config_name})
    if results is None:
        print('No results found.')
        return
    if json_format:
        print(json.dumps(results, indent=4))
    else:
        for result in results:
            recording = result['recording']
            sorter = result['sorter']
            recording_nwb_uri = result['recording_nwb_uri']
            sorting_true_npz_uri = result['sorting_true_npz_uri']
            sorting_npz_uri = result['sorting_npz_uri']
            sorting_console_lines_uri = result.get('sorting_console_lines_uri', None)
            comparison_with_truth_uri: dict = result['comparison_with_truth_uri']
            sorting_figurl = result.get('sorting_figurl', None)
            print('==================================================================')
            print(f'RECORDING: {recording["studyName"]}/{recording["name"]}')
            print(f'SORTER: {sorter["name"]}')
            print(f'Recording nwb: {recording_nwb_uri}')
            print(f'Sorting true npz: {sorting_true_npz_uri}')
            print(f'Sorting npz: {sorting_npz_uri}')
            print(f'Sorting console: {sorting_console_lines_uri}')
            print(f'Sorting figurl: {sorting_figurl}')
            print('')
            comparison_with_truth = kc.load_json(comparison_with_truth_uri)
            if comparison_with_truth is not None:
                for x in comparison_with_truth:
                    unit_id = x['unit_id']
                    # best_unit = x['best_unit']
                    accuracy = x['accuracy']
                    print(f'Unit {unit_id}: accuracy={accuracy}')
            else:
                print(f'File not found: {comparison_with_truth_uri}')
            print('')

if __name__ == '__main__':
    main()
