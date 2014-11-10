#!/usr/bin/env python
from optparse import OptionParser
import ConfigParser, os, sys, re, time, subprocess
import glob as glob
import datetime

# Define the options that are taken at runtime
parser = OptionParser()
parser.add_option("-x", "--eXecute", dest="execute", help="define config to run")

(options, args) = parser.parse_args()

# If runnning with configuration file as an argument, append the files to run over to a list and get the configuration parameters
if options.execute:
	config_name = options.execute
	if not os.path.exists(config_name):
		print "cannot find configuation:", config_name, ", exiting..."
		sys.exit()
		
	config = ConfigParser.RawConfigParser()
	config.read(config_name)
	
	cmssw_dir  = config.get('default', 'cmssw_dir')
	mass_point   = config.get('default', 'mass_point')
	output_dir = config.get('default', 'output_dir')
	identifier = config.get('default', 'identifier')
	walltime = config.get('job', 'walltime')
	npseudoexps = config.get('job', 'n_pseudoexps')
	splitting = config.get('job', 'splitting')
	systematic = config.get('systematics', 'systematic_variation')
	nfiles_to_submit = int(npseudoexps)/int(splitting)

else:
	print 'Please run in config mode [pserunner -x <config>]'
	sys.exit()

if not systematic:
	systematic = 'NOMINAL'

def run_over_files(file_iter):

	j = 0

	create_submission_script(out_area, file_iter)

	random_seed = 1+(file_iter-1)*int(npseudoexps)/nfiles_to_submit

	clusterconfig = open(out_area+"/samplesFile_"+str(file_iter)+".txt", 'w')
	for line in open(configname,'rb+'):
		if 'RANDOM_SEED' in line:
			clusterconfig.write("RANDOM_SEED: "+str(random_seed)+"\n")
		elif 'SIGNAL_FILE' in line and '#' not in line[0]:
			rfile = line.split(" ")[1]
			clusterconfig.write("SIGNAL_FILE: "+rfile.split("/")[-1]+" "+line.split(" ")[-2]+" "+systematic+"\n")
		elif 'BACKGROUND_FILE' in line and '#' not in line[0]:
			rfile = (line.split(" ")[1])
			clusterconfig.write("BACKGROUND_FILE: "+rfile.split("/")[-1]+" "+line.split(" ")[-2]+" "+systematic+"\n")
		else:
			clusterconfig.write(line)
		
	clusterconfig.close()

	for submission_script in sorted(glob.glob(out_area+'/*_'+str(file_iter)+'.sh')):
		command = "qsub -l walltime="+walltime+" -o "+out_area+" -e "+out_area+" -q localgrid@cream02 "+submission_script
		print command
		jobid = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		print jobid.stdout.readline().rstrip('\n')
		

# create output directory for submission
def create_directory(identifier, output_dir):
    date = time.strftime("%Y%m%d")
    dir_name = date+"_"+identifier
    dir_to_check = output_dir+"/"+dir_name
    if os.path.exists(dir_to_check):
        date = time.strftime("%Y%m%d_%H%M%S")
        dir_name = date+"_"+identifier

    os.system("mkdir -p "+output_dir+"/"+dir_name)

    return os.path.join(output_dir, dir_name)

# create submission script that is run on the node of the cluster
def create_submission_script(output_dir, file_iter):

	here,there = output_dir.split('/output')

	outfile = output_dir+"/submission_mass_"+mass_point+"_"+str(file_iter)+".sh";
	sub = open(outfile, "w") 

	local_node_directory = "/scratch"

	print >>sub, '''
#!/bin/bash
'''
	print >>sub, "source $VO_CMS_SW_DIR/cmsset_default.sh"
	print >>sub, "cd /localgrid/mccartin/CMSSW_5_3_11_patch2/src"
	print >>sub, "eval `scramv1 runtime -sh`"
	print >>sub, "mkdir -p "+local_node_directory+"/`whoami`/$PBS_JOBID"
	print >>sub, "cd "+local_node_directory+"/`whoami`/$PBS_JOBID"
	print >>sub, "cp -r "+output_dir+"/data ."
	print >>sub, "cp "+output_dir+"/runPseudoExperiments ."
	print >>sub, "cp "+output_dir+"/samplesFile_"+str(file_iter)+".txt ."
	print >>sub, "cp "+output_dir+"/LHCOTree*.root ."
            
	print >>sub, "time ./runPseudoExperiments samplesFile_"+str(file_iter)+".txt "+str(int(npseudoexps)/nfiles_to_submit)
	print >>sub, "ls -alh ."
	print >>sub, "cp MassJES_Bkg_M_"+mass_point+".5_JES_1_"+systematic+".root "+output_dir+"/MassJES_Bkg_M_"+mass_point+".5_JES_1_"+systematic+"_"+str(file_iter)+".root"
	print >>sub, "rm -rf "+local_node_directory+"/`whoami`/$PBS_JOBID"

	sub.close()
	os.system('chmod 755 '+outfile)
	return outfile


out_area = create_directory(identifier+"_mass_"+mass_point+"_"+systematic, output_dir)
print 'Creating submission scripts in directory \n '+str(out_area)

executable = '/user_mnt/user/mccartin/CMSSW/CMSSW_5_3_18/bin/slc6_amd64_gcc472/runPseudoExperiments'
configname = cmssw_dir+'/data/samplesFile_'+mass_point+'.txt'

rootinput_bkgd = []

for line in open(configname,'rb+'):
	if 'SIGNAL_FILE' in line:
		rootinput_sig = line.split(" ")[1]
	if 'BACKGROUND_FILE' in line and '#' not in line[0]:
		rootinput_bkgd.append(line.split(" ")[1])

print configname
os.system("mkdir "+out_area+"/data")
os.system("cp "+configname+" "+out_area)
os.system("cp "+cmssw_dir+"/data/muon_calibration.root "+out_area+"/data")
os.system("cp "+cmssw_dir+"/data/crossSection.root "+out_area+"/data")
os.system("cp "+rootinput_sig+" "+out_area)
for file in rootinput_bkgd:
	os.system("cp "+file+" "+out_area)
os.system("cp "+executable+" "+out_area)

print 'Submitting jobs...\n'

for i in range(1, nfiles_to_submit+1):
	run_over_files(i)
