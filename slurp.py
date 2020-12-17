#!/usr/bin/env python3

""" slurp
Usage:
     slurp -h|--help
     slurp file --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMANDFILE
     slurp command --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMAND

Options:
    -h --help                   Help
    --partition=<p>             Partition to run on. eg. clincloud, clincloud-himem, clincloud-express, skylake [default: clincloud]
    --account=<a>               Account to use. eg. gottgens-ccld-sl2-cpu, gottgens-ccld-sl3-cpu [default: gottgens-ccld-sl2-cpu]
    --email=<emailid>           Email id to use. eg. vs401 [default: vs401]
    --time=<time>               Upper time limit for the job D-HH:MM:SS [default: 12:00:00]
    --memory=<mem>              Memory in MB. If -1 then it will take the default value in the partition [default: -1]
    --nodes=<nodes>             Number of nodes [default: 1]
    --ntasks=<ntasks>           Number of tasks (cores) [default: 1]
    --jobname=<jobname>         Name of the job. Will be appended to logs.
    --log=<log>                 log file. Both stdout and stderr are written in the same file. [default: default]
"""
from docopt import docopt
import time
import os
import pandas as pd
import subprocess
import sys

class Slurmjob:
    def __init__(self, docopt_dict):
        jobd = dict()
        for key in docopt_dict:
            k = key.replace('--', '')
            if isinstance(docopt_dict[key], str):
                docopt_dict[key] = docopt_dict[key].replace("'", "")
            jobd[k] = docopt_dict[key]
        jobd['invoke_time'] = time.strftime('%Y%m%d-%H%M%S-%s', time.localtime())
        jobd['creation_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime())
        jobd['runid'] =  jobd['invoke_time'] + "-" + jobd['jobname']
        jobd['slurmcode_dir'] = 'jobfiles/'
        jobd["slurm_file"] = jobd['slurmcode_dir'] + jobd['runid'] + ".slurm"
        if jobd['log'] == 'default':
            jobd['log'] = ".slurm/" + jobd['runid'] + ".log"
        jobd['memory'] = int(jobd['memory'])
        jobd['cmdline'] = sys.argv
        self.job = jobd

    def job_details(self):
        return (self.job)

    def start_job(self):
        cmd = ['sbatch', self.job['slurm_file']]
        if self.job['file'] == True:
            if os.path.exists(self.job['COMMANDFILE']):
                shellout = subprocess.run(cmd, capture_output=True)
            else:
                print("Error: No input file present")
                exit(1)
        if self.job['command'] == True:
            shellout = subprocess.run(cmd, capture_output=True)
        self.job['shellout'] = shellout
        s = shellout.stdout
        slurmjob_id = s.split()[3].decode("utf-8")
        self.job['slurmjob_id'] = slurmjob_id
            #print(shellout)
        # if self.job['local'] == True:
        ## To do
        #    cmd = self.job['COMMAND']

    def write_job(self):
        """
        Writes a slurm job file
        :return: self
        """
        jobd = self.job
        if not os.path.exists(jobd['slurmcode_dir']):
            os.makedirs(jobd['slurmcode_dir'])
        command = ["#!/bin/bash",
                   "#SBATCH -p " + jobd['partition'],
                   "#SBATCH -A " + jobd['account'],
                   "#SBATCH -N " + jobd['nodes'],
                   "#SBATCH -n " + jobd['ntasks'],
                   "#SBATCH --job-name " + jobd['jobname'],
                   "#SBATCH --output " + jobd['log'],
                   "#SBATCH --error " + jobd['log'],
                   "#SBATCH --mail-type BEGIN,END,FAIL",
                   "#SBATCH --mail-user " + jobd['email'],
                   "#SBATCH --time " + jobd['time'],
                   "#SBATCH -p " + jobd['partition']
                   ]
        if jobd['memory'] != -1:
            command.append("#SBATCH --mem " + str(jobd['memory']))

        if jobd['file'] == True:
            command.append("bash " + jobd["COMMANDFILE"])
        if jobd['command'] == True:
            command.append(jobd["COMMAND"])
        # slurm_file = jobd['jobname'] + "-" + jobd['invoke_time'] + ".slurm"
        slurm_file = jobd['slurm_file']
        #print(f"Creating slurm file: {slurm_file}")

        with open(slurm_file, "w") as f:
            f.writelines("\n".join(command))
        jobd['slurm_file'] = slurm_file
        jobd['cwd'] = os.getcwd()
        self.job = jobd

    def _recorder(self, recordfile, type = "all"):
        jobd = self.job
        if type == "all":
            if os.path.exists(recordfile):
                fmod = "a"
                h = False
            else:
                fmod = "w"
                h = True
            df = pd.DataFrame.from_dict([jobd])
            df.to_csv(recordfile, header=h, index=False, mode=fmod)
        if type == 'cmd':
            if os.path.exists(recordfile):
                fmod = "a"
                h = False
            else:
                fmod = "w"
                h = True
            rid = jobd['runid']
            cm = " ".join(jobd['cmdline'])
            df = pd.DataFrame({'slurmjob_id': jobd['slurmjob_id'], 'runid':rid, 'created': jobd['creation_time'], 'invoked': jobd['invoke_time'], 'cwd': jobd['cwd'], 'cmd': cm }, index = [jobd['slurm_file']])
         #   df = pd.DataFrame({'runid': 1, 'cmd': 2 }, index=[1])
            df.to_csv(recordfile, header=h, index=False, mode=fmod)


    def _create_dirs(self):
        jobd = self.job
        home_dir = os.getenv("HOME")
        slurmdir = ".slurm"
        local_recorddir = ".slurp"
        main_recorddir = home_dir + "/.slurp_main"
        if not os.path.exists(slurmdir):
            os.makedirs(slurmdir)
        if not os.path.exists(local_recorddir):
            os.makedirs(local_recorddir)
        # main_recorddir=home_dir + "/.slurp_main"
        if not os.path.exists(main_recorddir):
            os.makedirs(main_recorddir)
        return [home_dir, slurmdir, local_recorddir, main_recorddir]

    def record_job(self):
        home_dir, slurmdir, local_recorddir, main_recorddir = self._create_dirs()
        jobd = self.job
        # recordfile=".slurp/hist.cmds"
        self._recorder(recordfile=local_recorddir + "/hist.cmds")
        # recordfile = ".slurp/hist.cmds"
        self._recorder(recordfile=main_recorddir + "/hist.cmds")
        self._recorder(recordfile=local_recorddir + "/cmdline.txt", type='cmd')
        self._recorder(recordfile=main_recorddir + "/cmdlile.txt", type='cmd')

    def copy_code(self):
        jobd = self.job
        code_dir = ".slurm/codes/"
        if not os.path.exists(code_dir):
            os.makedirs(code_dir)
        if jobd['file']:
            subprocess.run(['cp', jobd['COMMANDFILE'], code_dir + jobd['runid'] + ".code"],
                           capture_output=True)
        if jobd['command']:
            with open(code_dir + jobd['runid'] + ".code", "w") as f:
                f.writelines(jobd['COMMAND'])


if __name__ == '__main__':
    jobd = dict()
    #print(sys.argv)
    # arguments = docopt(__doc__, version='batch cmd v1.0', argv=["file", "--jobname='hello'",'outpt.txt'])
    arguments = docopt(__doc__, version='slurp v1.0')
    j = Slurmjob(arguments)
    j.write_job()
    j.start_job()
    j.record_job()
    j.copy_code()

    ## Some printing ##
    print(f"Slurp job id: {j.job['runid']}")
    print(f"Slurm job id: {j.job['slurmjob_id']}")
    print(f"Slurm job stdout/stderr: {j.job['log']}")
