#!/usr/bin/env python3

""" slurp
Usage:
     slurp -h|--help
     slurp file --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMANDFILE
     slurp command --jobname=<job-name> [ --partition=<p> ] [ --account=<A> ] [ --time=<D-HH:MM:SS> ] \
[--memory=<MB>] [--nodes=<N>] [--ntasks=<n>] [--log=<logfile>] [--email=<email>] COMMAND
    slurp ls [--head=<N>] [--tail=<N>] [--grep=<pattern>] [--type=<type>]

Options:
    -h --help                   Help
    -p --partition=<p>             Partition to run on. eg. skylake-himem, skylake [default: skylake]
    -a --account=<a>               Account to use. eg. gottgens-sl2-cpu, gottgens-sl3-cpu [default: gottgens-sl2-cpu]
    -e --email=<emailid>           Email id to use. eg. vs401 [default: vs401]
    -t --time=<time>               Upper time limit for the job D-HH:MM:SS [default: 12:00:00]
    -m --memory=<mem>              Memory in MB. If -1 then it will take the default value in the partition [default: -1]
    -n --nodes=<nodes>             Number of nodes [default: 1]
    -k --ntasks=<ntasks>           Number of tasks (cores) [default: 1]
    -j --jobname=<jobname>         Name of the job. Will be appended to logs.
    -l --log=<log>                 log file. Both stdout and stderr are written in the same file. [default: default]
    -d --head=<N>                  Top <N> lines of the file
    -i --tail=<N>                  Bottom <N> lines of the file
    -g --grep=<pattern>            Pattern you want to search for in the file
    -y --type=<type>               Can take either main or local. If main list all slurp commands. If local prints out slurp history in local dir.
"""
from docopt import docopt

class Slurmjob:
    def __init__(self, docopt_dict):
        jobd = dict()
        for key in docopt_dict:
            k = key.replace('--', '')
            if isinstance(docopt_dict[key], str):
                docopt_dict[key] = docopt_dict[key].replace("'", "")
            jobd[k] = docopt_dict[key]
        if jobd['jobname'] is None:
            jobd['jobname'] = str(random.randint(10000,90000))
            print("Job name not defined. Generating random one " + jobd['jobname'])
        jobd['invoke_time'] = time.strftime('%Y%m%d-%H%M%S-%s', time.localtime())
        jobd['creation_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime())
        jobd['runid'] =  jobd['invoke_time'] + "-" + jobd['jobname']
        jobd['slurmcode_dir'] = 'jobfiles/'
        jobd["slurm_file"] = jobd['slurmcode_dir'] + jobd['runid'] + ".slurm"
        if jobd['log'] == 'default':
            jobd['log'] = ".slurm/" + jobd['runid'] + ".log"
        jobd['memory'] = int(jobd['memory'])
        jobd['cmdline'] = sys.argv
        jobd['homedir'] = os.getenv("HOME")
        self.job = jobd

    def job_details(self):
        return (self.job)

    def start_job(self):
        cmd = ['/usr/local/software/slurm/current/bin/sbatch', self.job['slurm_file']]

        #### command == ls
        if self.job['ls'] == True:
            df = None
            if self.job['type'] == "main":
                main_file_name = self.job['homedir'] + '/.slurp_main/cmdline.txt'
                df = self._read_csv(main_file_name, sep=",")
                #if not df is None:
                #    print(df)
                df = self._read_csv(main_file_name)
            else:
                main_file_name = '.slurp/cmdline.txt'
                df = self._read_csv(main_file_name)
            if not df is None:
                print(df.to_markdown())
                    #print(df)
            exit(0)

        #### command == file
        if self.job['file'] == True:
            if os.path.exists(self.job['COMMANDFILE']):
                shellout = subprocess.run(cmd, capture_output=True)
            else:
                print("Error: No input file present")
                exit(1)

        #### command == command line
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

    def _read_csv(self, main_file_name, sep=","):
        try:
            df = pd.read_csv(main_file_name, sep=sep)
        except IOError:
            print("File is absent")
            print("There is no main file in this directory")
            df = None
        return(df)

    def write_job(self):
        """
        Writes a slurm job file
        :return: self
        """
        jobd = self.job

        if jobd['command'] == 'ls':
            return

        if not os.path.exists(jobd['slurmcode_dir']):
            os.makedirs(jobd['slurmcode_dir'])
        command = ["#!/bin/bash",
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
        self._recorder(recordfile=main_recorddir + "/cmdline.txt", type='cmd')

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
    arguments = docopt(__doc__, version='slurp v1.0')
    import time
    import os
    import pandas as pd
    import subprocess
    import sys
    import random

    j = Slurmjob(arguments)
    j.write_job()
    j.start_job()
    j.record_job()
    j.copy_code()

    ## Some printing ##
    print(f"Slurp job id: {j.job['runid']}")
    print(f"Slurm job id: {j.job['slurmjob_id']}")
    print(f"Slurm job stdout/stderr: {j.job['log']}")
