# spikeforest-workflow

Compare the accuracy of spike sorting algorithms on electrophysiolical recordings with ground truth firing events.

## Prerequisites

* Python >= 3.8
* [A running kachery daemon](https://github.com/kacheryhub/kachery-doc/blob/main/doc/hostKacheryNode.md)
* Docker (unless you want to run the spike sorting outside of docker)
* SpikeInterface (`pip install spikeinterface`)
* nwb_conversion_tools (install [Python package from source](https://github.com/catalystneuro/nwb-conversion-tools) for now)
* runarepo (`pip install git+https://github.com/scratchrealm/runarepo`)

## Running the workflow

The SpikeForest workflow are split into a collection of scripts. These scripts are meant to be run repeatedly, as the updates from one will influence the others. These services all read and write to the local kachery node via the running daemon.

The main script is `workflow.py`. This assembles a list of jobs and a list of results. This script should be run before the other scripts are run, and then again any time jobs have been newly completed by the other scripts. To run with the test configuration:

```bash
scripts/workflow.py test.yaml
```

Once the workflow script has completed you can run the prepare scripts

```bash
scripts/prepare_recording_nwb.py test.yaml
scripts/prepare_sorting_true_npz.py test.yaml
```

Next, run the spike sorters (see [spikesorting-runarepo](https://github.com/scratchrealm/spikesorting-runarepo))

```bash
scripts/sorting.py test.yaml mountainsort4 --docker
scripts/sorting.py test.yaml spykingcircus --docker
```

Then the comparison with truth

```bash
scripts/compare_with_truth.py test.yaml --docker
```

Finally, run the workflow script once again to collect the results

```bash
scripts/workflow.py test.yaml
```