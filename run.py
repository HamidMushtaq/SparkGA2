#!/usr/bin/python
#****************************************/
#	Script:		run.py	
#	Author:		Hamid Mushtaq  		
#	Company:	TU Delft	 	
#****************************************/
from xml.dom import minidom
import sys
import os
import time

exeName = "dnaseqanalyzer_2.11-1.0.jar"

USE_YARN_CLIENT_FOR_HADOOP = True

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example of usage: ./run.py config_file.xml 1")
	sys.exit(1)
	
configFilePath = sys.argv[1]
part = sys.argv[2]

if not os.path.isfile(configFilePath):
	print("Config file " + configFilePath + " does not exist!")
	sys.exit(1)
 
doc = minidom.parse(configFilePath)
mode = doc.getElementsByTagName("mode")[0].firstChild.data
refPath = doc.getElementsByTagName("refPath")[0].firstChild.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
tmpFolder = doc.getElementsByTagName("tmpFolder")[0].firstChild.data
numExecs = doc.getElementsByTagName("numExecs")[0].firstChild.data
execTasks = doc.getElementsByTagName("execTasks")[0].firstChild.data
exe_mem = doc.getElementsByTagName("execMemGB")[0].firstChild.data + "g"
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

print "mode = |" + mode + "|"
	
def runHadoopMode(refPath, numExecs):
	dictHDFSPath = refPath.replace(".fasta", ".dict")
	dictPath = './' + dictHDFSPath[dictHDFSPath.rfind('/') + 1:]
	
	if USE_YARN_CLIENT_FOR_HADOOP:
		if not os.path.exists(tmpFolder):
			os.makedirs(tmpFolder)
	
	if not os.path.exists(dictPath):
		os.system("hadoop fs -get " + dictHDFSPath)
	if int(part) == 1:
		os.system("hadoop fs -rm -r -f " + outputFolder)
	
	diff_str = "yarn-client" if USE_YARN_CLIENT_FOR_HADOOP else ("yarn-cluster --files ./config.xml," + dictPath)
		
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master " + diff_str + " " + \
	"--driver-memory " + driver_mem + " --executor-memory " + exe_mem + " " + \
	"--num-executors " + numExecs + " --executor-cores " + execTasks + " " + \
	exeName + " " + part
	
	print cmdStr
	os.system(cmdStr)

def runLocalMode():
	if int(part) == 1:
		os.system("rm -r -f " + outputFolder)
	
	if not os.path.exists(tmpFolder):
		os.makedirs(tmpFolder)
	os.system("rm * " + tmpFolder)
	
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--jars lib/htsjdk-1.143.jar " + \
	"--class \"DNASeqAnalyzer\" --master local[*] --driver-memory " + driver_mem + " " + exeName + " " + part
	
	print cmdStr
	os.system(cmdStr)
	
start_time = time.time()

os.system('cp ' + configFilePath + ' ./config.xml')
if (mode == "local"):
	runLocalMode()
else:
	runHadoopMode(refPath, numExecs)
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "||| Time taken = " + str(mins) + " mins " + str(secs) + " secs |||"
# Save the results to file
f = open("results.txt",'a+')
f.write(str(time_in_secs) + "\t" + str(mins) + "\t" + str(secs) + "\n")
f.close() 
