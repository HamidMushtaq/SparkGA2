#!/usr/bin/python
#****************************************/
#	Script:		runAll.py	
#	Author:		Hamid Mushtaq  		
#****************************************/
from xml.dom import minidom
import sys
import os
import time

logFile = "time.txt"
configFile = sys.argv[1]
startingPart = 1

doc = minidom.parse(configFile)
outputFolder = doc.getElementsByTagName("outputFolder")[0].firstChild.data

if len(sys.argv) > 2: 
	startingPart = int(sys.argv[2])

if startingPart == 1:	
	if os.path.exists(logFile):
		os.remove(logFile)

def addToLog(s):
	f = open(logFile,'a+')
	f.write(s + "\n")
	f.close() 
	
def getElapsedStr(elapsed):
	return str(elapsed / 60) + " mins " + str(elapsed % 60) + " secs"

times_list = []
times_list.append(time.time())

if startingPart == 1:
	os.system("hadoop fs -rm -r .Trash/*")

iterations = 4 - startingPart
for i in range(0, iterations):
	part = str(i+startingPart)
	os.system("./runPart.py " + configFile + " " + part + " &> log" + part + ".txt")
	pt = times_list[i]
	ct = time.time()
	times_list.append(ct)
	addToLog(">> Part" + part + " took " + getElapsedStr(int(ct-pt)))
	addToLog("--------------------------------------")

totalTime = getElapsedStr(int(times_list[iterations]-times_list[0]))
addToLog(">> From part " + str(startingPart) + " till end, it took " + totalTime)
print "<< From part " + str(startingPart) + " till end, it took " + totalTime + " >>"
