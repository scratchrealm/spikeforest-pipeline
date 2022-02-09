#!/usr/bin/env python3

import sys
import os
import yaml
import sorting

def main():
    rank = os.getenv('SLURM_ARRAY_TASK_ID')
    if not rank:
        raise Exception('Not running as slurm task. Exiting.')
    real_rank = int(rank) - 1
    with open(sys.argv[1], 'r') as f:
        metaconfig = yaml.safe_load(f)
    my_config = metaconfig[real_rank]
    
    child_config = my_config['custom_config_path']
    sorter = my_config['sorter']
    max_sim = int(my_config.get('max_simultaneous_sorts', 1))
    force_run = my_config.get('force_run', False)
    container = my_config.get('container_path')
    docker = my_config.get('docker', False) and (container is not None)
    singularity = my_config.get('singularity', False) and (container is not None)

    if (child_config is None or sorter is None):
        raise Exception(f'Incorrect meta-configuration for worker rank {rank}')
    sorting.main(
        child_config,
        sorter,
        max_sim,
        force_run,
        docker,
        singularity,
        container
    )

if __name__ == "__main__":
    main()