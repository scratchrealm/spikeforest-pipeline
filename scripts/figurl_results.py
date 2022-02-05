#!/usr/bin/env python3

import os
import click
import yaml
import kachery_client as kc
import figurl

@click.command()
@click.argument('config_file')
def main(config_file: str):
    if not os.environ.get('FIGURL_CHANNEL'):
        raise Exception(f'Environment variable not set: FIGURL_CHANNEL')
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    config_name = config['name']
    print(f'Config name: {config_name}')
    results = kc.get({'type': 'spikeforest-workflow-results', 'name': config_name})
    for result in results:
        result['comparison_with_truth'] = kc.load_json(result['comparison_with_truth_uri'])
    F = figurl.Figure(
        data={'type': 'spikeforest-workflow-results', 'results': results},
        view_url='gs://figurl/spikeforestview-1'
    )
    url = F.url(label=f'SF Workflow results: {config_name}')
    print(url)

if __name__ == '__main__':
    main()
