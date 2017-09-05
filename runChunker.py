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

SAVE_EXEC_TIME = True

if len(sys.argv) < 3:
	print("Not enough arguments!")
	print("Example usage: ./run.py chunker_2.11-1.0.jar config.xml")
	sys.exit(1)

exeName	= sys.argv[1]
configFile = sys.argv[2]
doc = minidom.parse(configFile)
inputFileName = doc.getElementsByTagName("fastq1Path")[0].firstChild.data
fastq2Path = doc.getElementsByTagName("fastq2Path")[0].firstChild
inputFileName2 = "" if (fastq2Path == None) else fastq2Path.data
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data
driver_mem = doc.getElementsByTagName("driverMemGB")[0].firstChild.data + "g"

def run():
	cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	"--class \"hmushtaq.fastqchunker.Chunker\" --master local[*] --driver-memory " + driver_mem + " " + exeName + " " + configFile
	
	print cmdStr
	os.system(cmdStr)

start_time = time.time()
os.system("hadoop fs -rm -r " + outputFolder)
run()

time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60
print "||| Time taken = " + str(mins) + " mins " + str(secs) + " secs |||"
if SAVE_EXEC_TIME:
	timingsFileName = "timings.txt"
	if not os.path.exists(timingsFileName):
		f = open(timingsFileName,'w')
		f.write("%Config file\tTime in secs\tTime in mins:secs\n\n")
		f.close
	f = open(timingsFileName,'a+')
	f.write(sys.argv[2] + "\t" + str(time_in_secs) + "\t" + str(mins) + ":" + str(secs) + "\n")
	f.close() 
