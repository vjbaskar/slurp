# slurp
A convenient way to run jobs in slurm.

Just clone the repo and `ln -s ~/clonedir/slurp/slurp.py ~/bin/slurp` and change file permissions to make it executable.

## Dependencies
python >= 3.7, time, os, pandas, subprocess, docopt, sys, subprocess

## Subcommands
```
slurp file [options] command_bash_file.sh
slurp command [options] "samtools index file.bam"
```
One of the above commands submits a job using `sbatch` to HPC.

It creates the following files:
* `./jobfiles/date-time-seconds.slurm` file.
* `./slurm/date-time-seconds.log` file which is the slurm stdout/stderr.
* `.slurp/cmdline.txt` containing all the commands that you ran from the current directory
* `$HOME/.slurp_main/cmdline.txt` contain all the commands ever run using slurp command in your home directory.

## Example

```
slurp command --jobname='bamsort' --time=02:00:00 "singularity ~/sifs/samtools.sif samtools index input.bam"
```
This runs by default on the partition, with default memory for 1 core for 2hrs.
