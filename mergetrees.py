#!/usr/bin/env python

import sys,os
import numpy as np
import glob

import ROOT

# Set ROOT options
ROOT.TH1.SetDefaultSumw2(False)
ROOT.gStyle.SetOptStat(0)

pentuple_dir = sys.argv[1]


rfile_list = sorted(glob.glob(pentuple_dir+'Mass*NOMINAL_*.root'))
script_list = sorted(glob.glob(pentuple_dir+'*.sh'))
out_list = sorted(glob.glob(pentuple_dir+'*.sh.o*'))
err_list = sorted(glob.glob(pentuple_dir+'*.sh.e*'))
config_list = sorted(glob.glob(pentuple_dir+'samplesFile*.txt'))

rlist_1 = ROOT.TList()

rfiles = []

bad_files = 0

# Check for valid input root files
if len(rfile_list) < 2:
	print 'No valid input files found!'
	sys.exit()

# Check output files for seg faults and walltime issues
for file in err_list:
	for line in open(file, 'rb+'):
		if ('segmentation' in line) or ('walltime' in line):
			print 'Warning!  Errors found in job: '+str(file)
			bad_files += 1

if bad_files != 0:
	harvest = raw_input('Do you wish to continue? [y/N]\n')
	if harvest == 'y':
		harvest = True
	else:
		harvest = False
else:
	harvest = True

if harvest:
	
	files_dict = {}

	print 'Will merge from '+str(len(rfile_list))+' files.'

	# Read in the trees from the root file
	for file in rfile_list:
		id = int([value for value in file.split('_')][-1].strip('.root'))
		files_dict[id] = 1
		rfile = ROOT.TFile(file)
		rfiles.append(rfile)
		psexpTree = rfile.Get('psexpTree')
		psexpInfoTree = rfile.Get('psexpInfoTree')	
		
		rlist_1.Add(psexpTree)

	# Check for missing root files
	for i in range(1,len(rfile_list)):
		if i not in files_dict:
			print 'Missing root file from job '+str(i)+'!'

	mergedInfoTree = psexpInfoTree.CloneTree(0)

	outfile = rfile_list[0].split('NOMINAL_')[0]+'NOMINAL.root'

	mergedFile = ROOT.TFile(outfile,'recreate')

	print 'Merging trees'
	mergedTree = ROOT.TTree().MergeTrees(rlist_1)
	mergedTree.SetName('psexpTree')


	numPsexp = np.zeros(1, dtype=np.float32)
	mergedInfoTree.Branch('numPsexp', numPsexp, 'numPsexp/F')

	psexpInfoTree.GetEntry(0)
	numPsexp[0] = np.float32(len(rfile_list)*psexpInfoTree.numPsexp)

	mergedInfoTree.Fill()
	mergedInfoTree.Write()

	mergedFile.Write()

	print 'Merged '+str(len(rfile_list)*psexpInfoTree.numPsexp)+' pseudoexperiments to '+str(outfile)+'\n'

	cleanup = raw_input('Clean up output files? [y/N]\n')
	if cleanup == 'y':
		cleanup = True
	else:
		cleanup = False
	
	if cleanup:
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
