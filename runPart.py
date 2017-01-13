#!/usr/bin/python
#****************************************/
#	Script:		runPart.py	
#	Author:		Hamid Mushtaq  			
#****************************************/
from xml.dom import minidom
import sys
import os
import time
import subprocess
import math
import glob

USE_YARN_CLIENT_FOR_HADOOP = False

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example of usage: ./run2.py config.xml 1")
	sys.exit(1)

exeName = "dnaseqanalyzer_2.11-1.0.jar"
logFile = "time.txt"
configFilePath = sys.argv[1]
partNumber = sys.argv[2]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)

doc = minidom.parse(configFilePath)
mode = doc.getElementsByTagName("mode")[0].firstChild.data
refPath = doc.getElementsByTagName("refPath")[0].firstChild.data
inputFolder = doc.getElementsByTagName("inputFolder")[0].firstChild.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
tmpFolder = doc.getElementsByTagName("tmpFolder")[0].firstChild.data
# Parameters for this part
numInstances = doc.getElementsByTagName("numInstances" + partNumber)[0].firstChild.data
numThreads = doc.getElementsByTagName("numTasks2")[0].firstChild.data if (int(partNumber) == 2) else "1"
exe_mem = doc.getElementsByTagName("execMemGB" + partNumber)[0].firstChild.data + "g"
double_exe_mem = str(int(doc.getElementsByTagName("execMemGB" + partNumber)[0].firstChild.data) * 2) + "g"
driver_mem = doc.getElementsByTagName("driverMemGB" + partNumber)[0].firstChild.data + "g"

print "mode = |" + mode + "|"

def getNumOfHadoopChunks():
	cat = subprocess.Popen(["hadoop", "fs", "-ls", inputFolder], stdout=subprocess.PIPE)
	count = 0.0
	for line in cat.stdout:
		if inputFolder in line:
			count = count + 1.0
	return count
	
def getNumOfLocalChunks():
	files = glob.glob(inputFolder + "/*.gz")
	return float(len(files))

def executeHadoop(part, ni, em, extra_param):	
	dictHDFSPath = refPath.replace(".fasta", ".dict")
	dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
	
	if USE_YARN_CLIENT_FOR_HADOOP:
		os.system('cp ' + configFilePath + ' ./')
		if not os.path.exists(tmpFolder):
			os.makedirs(tmpFolder)
	
	diff_str = "yarn-client" if USE_YARN_CLIENT_FOR_HADOOP else ("yarn-cluster --files " + configFilePath + "," + dictPath)
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master " + diff_str + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + em + " " + \
	"--num-executors " + ni + " --executor-cores " + numThreads + " " + \
	exeName + " " + os.path.basename(configFilePath) + " " + str(part) + extra_param
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)
	os.system(cmdStr)
	
	if USE_YARN_CLIENT_FOR_HADOOP:
		os.remove('./' + configFilePath[configFilePath.rfind('/') + 1:])
	
def executeLocal(part, extra_param):
	if not os.path.exists(tmpFolder):
		os.makedirs(tmpFolder)
			
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master local[*] --driver-memory " + driver_mem + " " + exeName + " " + \
	configFilePath + " " + str(part) + extra_param
	
	print cmdStr
	addToLog("[" + time.ctime() + "] " + cmdStr)   
	os.system(cmdStr)
	
def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 

def runHadoopMode(part):
	if part == 1:
		addToLog("########################################\n[" + time.ctime() + "] Part1 started.")
		dictHDFSPath = refPath.replace(".fasta", ".dict")
		dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
		if not os.path.exists(dictPath):
			os.system("hadoop fs -get " + dictHDFSPath)
		os.system("hadoop fs -rm -r -f " + outputFolder)
	
	executeHadoop(part, numInstances, exe_mem, "")
	addToLog("[" + time.ctime() + "] Part" + str(part) + " completed.")
	
def runLocalMode(part):
	if part == 1:
		addToLog("[" + time.ctime() + "] Part1 started.")
		if os.path.isdir(outputFolder):
			os.system("rm -r -f " + outputFolder)
		os.system("mkdir " + outputFolder)
		
	executeLocal(part, "")
	addToLog("[" + time.ctime() + "] Part" + str(part) + " completed.")
	if part == 3:
		os.system('mv ' + tmpFolder + '/*.vcf ' + outputFolder)
	
start_time = time.time()

if (mode == "local"):
	runLocalMode(int(partNumber))
else:
	runHadoopMode(int(partNumber))
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "|| Time taken = " + str(mins) + " mins " + str(secs) + " secs ||"
