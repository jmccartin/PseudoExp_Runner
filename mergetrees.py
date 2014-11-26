#!/usr/bin/env python

import sys,os
import numpy as np
import glob,re

import ROOT

# Set ROOT options
ROOT.TH1.SetDefaultSumw2(False)
ROOT.gStyle.SetOptStat(0)

pentuple_dir = sys.argv[1]
verbose = False

print 'Merging files in '+pentuple_dir+'...'

rfile_list = []
rfile_list_raw = sorted(glob.glob(pentuple_dir+'MassJES*_*.root'))
# Don't include any files already merged.  There's probably an easier way to code this.
for file in sorted(rfile_list_raw):
	if len(re.findall('_[0-9]{1,5}.root',file)) != 0:
		rfile_list.append(file)

script_list = sorted(glob.glob(pentuple_dir+'*.sh'))
out_list = sorted(glob.glob(pentuple_dir+'*.sh.o*'))
err_list = sorted(glob.glob(pentuple_dir+'*.sh.e*'))
config_list = sorted(glob.glob(pentuple_dir+'samplesFile*.txt'))

rlist_1 = ROOT.TList()

rfiles = []

bad_files = {}

# Resubmit jobs to the cluster with qsub
def resubmit_job(job):
	out_area = job.split('/submission_')[0]
	command = "qsub -l walltime="+walltime+" -o "+out_area+" -e "+out_area+" -q localgrid@cream02 "+job
	print command
	os.system(command)
	os.system('rm '+job+'.e*')
	os.system('rm '+job+'.o*')


# Check output files for seg faults and walltime issues
for file in err_list:
	for line in open(file, 'rb+'):
		if ('segmentation' in line) or ('walltime' in line) or ('is not a directory' in line):
			print 'Warning!  Errors found in job: '+str(file)
			bad_files[file.split('.e')[0]] = 1

if len(bad_files) != 0:
	harvest = raw_input('Do you wish to continue? [y/N]\n')
	if harvest == 'y':
		harvest = True
	else:
		harvest = False
else:
	harvest = True

if not harvest:
	resubmit = raw_input('Do you wish to resubmit bad jobs? [y/N]\n')
	if resubmit == 'y':
		walltime = raw_input('Please enter new walltime for jobs to submit: ')
		if walltime == '':
			walltime = '01:59:59'
		for job in bad_files.keys():
			resubmit_job(job)

if harvest:
	# Check for valid input root files
	if len(rfile_list) < 2:
		print 'No valid input files found!'
		sys.exit()

	files_dict = {}

	print 'Will merge from '+str(len(rfile_list))+' files.'

	# Read in the trees from the root file
	for i,file in enumerate(rfile_list):
		id = int([value for value in file.split('_')][-1].strip('.root'))
		files_dict[id] = 1
		rfile = ROOT.TFile(file)
		rfiles.append(rfile)
		psexpTree = rfile.Get('psexpTree')
		if i == 0:
			psexpInfoTree = rfile.Get('psexpInfoTree')	
		
		rlist_1.Add(psexpTree)

	# Check for missing root files
	missing_jobs = []
	for i in range(1,len(rfile_list)):
		if i not in files_dict:
			print 'Missing root file from job '+str(i)+'!'
			missing_jobs.append(i)

	#psexpInfoTree.GetEntry(0)
	#Npexp = len(rfile_list)*psexpInfoTree.numPsexp

	#print Npexp

	#mergedInfoTree.SetBranchStatus('numPsexp',0)
	#branch = mergedInfoTree.GetBranch('psexpTree')
	#mergedInfoTree.GetListOfBranches().Remove(branch)
	#leaf = mergedInfoTree.GetLeaf('psexpTree')
	#mergedInfoTree.GetListOfLeaves().Remove(leaf)

	#mergedInfoTree.Write()

	suffix = re.findall('_[0-9]{1,3}.root',rfile_list[0])
	outfile = rfile_list[0].split(suffix[0])[0]+'.root'

	#psexpInfoTree.SetBranchStatus('numPsexp',0)
	
	#mergedInfoTree = psexpInfoTree.CloneTree(0)
	#mergedInfoTree.CopyEntries(psexpInfoTree)

	mergedInfoTree = ROOT.TTree('psexpInfoTree','psexpInfoTree')

	mergedFile = ROOT.TFile(outfile,'recreate')

	print 'Merging trees'
	mergedTree = ROOT.TTree().MergeTrees(rlist_1)
	mergedTree.SetName('psexpTree')
	
	signalEvents = np.zeros(1, dtype=np.int)
	bkgEvents = np.zeros(1, dtype=np.int)
	numPsexp = np.zeros(1, dtype=np.int)
	signalPool = np.zeros(1, dtype=np.int)
	bkgPool = np.zeros(1, dtype=np.int)
	trueJES = np.zeros(1, dtype=np.float64)
	trueMass = np.zeros(1, dtype=np.float64)

	mergedInfoTree.Branch('numPsexp', numPsexp, 'numPsexp/I')
	mergedInfoTree.Branch('signalEvents', signalEvents, 'signalEvents/I')
	mergedInfoTree.Branch('bkgEvents', bkgEvents, 'bkgEvents/I')
	mergedInfoTree.Branch('signalPool', signalPool, 'signalPool/I')
	mergedInfoTree.Branch('bkgPool', bkgPool, 'bkgPool/I')
	mergedInfoTree.Branch('trueJES', trueJES, 'trueJES/D')
	mergedInfoTree.Branch('trueMass', trueMass, 'trueMass/D')

	for event in range(psexpInfoTree.GetEntries()):
		nBytes = psexpInfoTree.GetEntry(event)
		if nBytes<=0:
			continue
		Npexp = len(rfile_list)*psexpInfoTree.numPsexp
		numPsexp[0] = np.float32(Npexp)
		signalEvents[0] = psexpInfoTree.signalEvents
		bkgEvents[0] = psexpInfoTree.bkgEvents
		signalPool[0] = psexpInfoTree.signalPool
		bkgPool[0] = psexpInfoTree.bkgPool
		trueMass[0] = psexpInfoTree.trueMass
		trueJES[0] = psexpInfoTree.trueJES

		mergedInfoTree.Fill()

	#psexpInfoTree.GetEntry(0)
	#Npexp = len(rfile_list)*psexpInfoTree.numPsexp
	#numPsexp[0] = np.float32(Npexp)

	#mergedInfoTree.Fill()
	mergedInfoTree.Write()

	mergedFile.Write()

	if(verbose): # Don't turn this on, it spams too much.
    #filetree = pentuple.GetDirectory('LHCO').Get('Particle_tree')
		filetree = mergedFile.Get('psexpTree')
		for event in range(filetree.GetEntries()):
			nBytes = filetree.GetEntry(event)
			if nBytes<=0:
				continue
			print filetree.Scan()


	print 'Merged '+str(len(rfile_list)*psexpInfoTree.numPsexp)+' pseudoexperiments to '+str(outfile)+'\n'

	# If there are missing root files, resubmit them
	if len(missing_jobs) != 0:
		resubmit_missing_jobs = raw_input('Resubmit missing jobs? [y/N]\n')
		if resubmit_missing_jobs == 'y':
			walltime = raw_input('Please enter new walltime for jobs to submit: ')
			for file in script_list:
				job_id = int(file.split('.sh')[0].split('_')[-1])
				if job_id in missing_jobs:
					resubmit_job(file)
	else:
		# Clean up all split root files, job logs and config files
		cleanup = raw_input('Clean up output files? [y/N]\n')
		if cleanup == 'y':
			for file in rfile_list:
				os.system('rm '+file)
			for file in out_list:
				os.system('rm '+file)
			for file in err_list:
				os.system('rm '+file)
			for file in script_list:
				os.system('rm '+file)
			for file in config_list:
				os.system('rm '+file)
