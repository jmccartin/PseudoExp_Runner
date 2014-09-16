#!/usr/bin/env python

import sys,os
import numpy as np
import glob

import ROOT

# Set ROOT options
ROOT.TH1.SetDefaultSumw2(False)
ROOT.gStyle.SetOptStat(0)

pentuple_dir = sys.argv[1]


file_list = glob.glob(pentuple_dir+'Mass*.root')
rlist_1 = ROOT.TList()

for file in file_list:
	rfile = ROOT.TFile(file)
	psexpTree = rfile.Get('psexpTree')
	psexpInfoTree = rfile.Get('psexpInfoTree')	
	
	rlist_1.Add(psexpTree)


#rfile2 = ROOT.TFile(file_list[0])
#psexpInfoTree2 = rfile.Get('psexpInfoTree')	
mergedInfoTree = psexpInfoTree.CloneTree(0)


mergedFile = ROOT.TFile('test.root','recreate')

mergedTree = ROOT.TTree().MergeTrees(rlist_1)
mergedTree.SetName('psexpTree')


numPsexp = np.zeros(1, dtype=np.float32)
#signalEvents = np.zeros(1, dtype=np.float32)
#bkgEvents = np.zeros(1, dtype=np.float32)
#signalPool = np.zeros(1, dtype=np.float32)
#bkgPool = np.zeros(1, dtype=np.float32)
#trueJES = np.zeros(1, dtype=np.float32)
#trueMass = np.zeros(1, dtype=np.float32)

mergedInfoTree.Branch('numPsexp', numPsexp, 'numPsexp/F')

psexpInfoTree.GetEntry(0)
numPsexp[0] = np.float32(len(file_list)*psexpInfoTree.numPsexp)

mergedInfoTree.Fill()
mergedInfoTree.Write()

#mergedTree.Print()
#mergedInfoTree.Print()

mergedFile.Write()
