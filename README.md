# spikeforest-workflow

Compare the accuracy of spike sorting algorithms on electrophysiolical recordings with ground truth firing events.

## Prerequisites

* Python >= 3.8
* [A running kachery daemon](https://github.com/kacheryhub/kachery-doc/blob/main/doc/hostKacheryNode.md)
* Docker (unless you want to run the spike sorting outside of docker)
* SortingView (`pip install sortingview`)
* spikeinterface -- install [Python package from source](https://github.com/spikeinterface/spikeinterface)
* nwb_conversion_tools -- install [Python package from source](https://github.com/catalystneuro/nwb-conversion-tools)
* runarepo (`pip install git+https://github.com/scratchrealm/runarepo`)

In order to load the spikeforest datasets, you will need to [configure your kachery node to be part of the spikeforest channel](https://github.com/flatironinstitute/spikeforest/blob/main/doc/join-spikeforest-download-channel.md).

## Running the workflow

The SpikeForest workflow is split into a collection of scripts. These scripts are meant to be run multiple times, as the updates from one will influence the others. These scripts all read and write to the local kachery node via the running daemon.

The easiest way to get started is to run the example in [devel/test-docker](devel/test-docker) under this repo base:

```bash
cd spikeforest-workflow/devel/test-docker
```

See the contents of [config.yaml](devel/test-docker/config.yaml) for which recordings and sorters will be used in this workflow.

Start by running the workflow script to assemble the list of jobs to be run

```bash
./workflow
```

Next download and prepare the spikeforest datasets

```bash
./prepare
```

You must run the workflow script again to assemble the updated jobs

```bash
./workflow
```

Now run the spike sorting:

```bash
./mountainsort4
./spykingcircus
./workflow
```

Compare with truth:

```bash
./compare
./workflow
```

Prepare figurl views for the sorting outputs. Note that you will need to set the FIGURL_CHANNEL environment variable to a channel for which you have file upload permissions.

```bash
./sorting-figurl
./workflow
```

Print the results

```bash
./print-results
```

Finally, generate the figurl link to the results of the workflow. Once again, you will need to set the FIGURL_CHANNEL environment variable to a channel for which you have file upload permissions.

```bash
./results-figurl
```

This will print a URL. For example:

https://figurl.org/f?v=gs://figurl/spikeforestview-1&d=73e428abc3b8ad627fe4faa702318ba67b6f39a3&channel=flatiron1&label=SF%20workflow%20results%3A%20test-docker
