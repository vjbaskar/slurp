# slurp

`slurp` submits shell commands and command files to a Slurm cluster using `sbatch`.

## Requirements

- Python 3.7 or newer
- A Slurm system with `sbatch` at `/usr/local/software/slurm/current/bin/sbatch`
- A valid Slurm account and partition

## Install

Install from a clone of this repository:

```sh
python -m pip install .
```

Or install directly from GitHub:

```sh
python -m pip install git+https://github.com/vjbaskar/slurp.git
```

For development, install the working tree in editable mode:

```sh
python -m pip install -e .
```

Pip installs the `slurp` command and its Python dependencies. Check the command
and available options with `slurp --help`.

## Usage

Submit a command file:

```sh
slurp file --jobname=analysis run_analysis.sh
```

Submit a command string:

```sh
slurp command --jobname=bamsort --time=02:00:00 \
	"samtools index input.bam"
```

View recorded commands:

```sh
slurp ls --type=local
slurp ls --type=main
```

`local` shows history for the current directory; `main` shows history across
directories for the current user.

### Options

| Option | Default | Description |
| --- | --- | --- |
| `--jobname` | Required | Slurm job name |
| `--partition` | `skylake` | Slurm partition |
| `--account` | `gottgens-sl2-cpu` | Slurm account |
| `--email` | `vs401` | Address or cluster-specific email identifier for Slurm notifications |
| `--time` | `12:00:00` | Time limit (`D-HH:MM:SS`) |
| `--memory` | `-1` | Memory in MB; `-1` uses the partition default |
| `--nodes` | `1` | Number of nodes |
| `--ntasks` | `1` | Number of tasks/cores |
| `--log` | Generated path | File for Slurm standard output and error |

Short option aliases are available; run `slurp --help` for the full list.

## Generated files

Running jobs creates files relative to the current directory:

- `jobfiles/`: generated Slurm submission scripts
- `.slurm/`: job logs and copies of submitted code
- `.slurp/`: command history for the current directory
- `$HOME/.slurp_main/`: command history across directories for the current user
