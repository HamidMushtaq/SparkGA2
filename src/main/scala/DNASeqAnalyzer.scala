/*
 * Copyright (C) 2016-2017 Hamid Mushtaq
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import sys.process._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.util.Sorting._
import scala.concurrent.Future
import scala.concurrent.forkjoin._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Random

import tudelft.utils._
import utils._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._

object DNASeqAnalyzer 
{
// Flags
final val UseNewLoadBalancer = true
final val ShowMeTheRegions = false
final val saveAllStages = false
final val writeToLog = true
final val compressRDDs = true
final val useBWALogger = true
final val showNumOfBytes = false
// Downloading
final val downloadNeededFiles = false
// Compression and reading of HDFS files
final val unzipBWAInputDirectly = false
final val readLBInputDirectly = true
// Optional stages
final val doIndelRealignment = false
final val doPrintReads = false

final val SF = 1e12.toLong
//////////////////////////////////////////////////////////////////////////////
def bwaRun (chunkName: String, config: Configuration) : Array[(Long, Int)] = 
{
	val inputFileIsUnzipped = !chunkName.contains(".gz")
	val x = chunkName.replace(".gz", "")
	val gz = if (inputFileIsUnzipped) "" else ".gz"
	var inputFileName = if ((config.getMode != "local") && !unzipBWAInputDirectly && !inputFileIsUnzipped) (config.getTmpFolder + x + gz) else 
		(config.getInputFolder + x + gz)
	val fqFileName = config.getTmpFolder + x
	val mtFileWrite = false
	val hdfsManager = new HDFSManager
	
	var t0 = System.currentTimeMillis
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
		{
			hdfsManager.create(config.getOutputFolder + "log/bwa/" + x)
		}
		downloadBinProgram("bwa", config)
		val file = new File(getBinToolsDirPath(config) + "bwa") 
		file.setExecutable(true)
		if (downloadNeededFiles)
		{
			dbgLog("bwa/" + x, t0, "download\tDownloading reference files for bwa", config)
			downloadBWAFiles("dlbwa/" + x, config)
		}
	}
	
	dbgLog("bwa/" + x, t0, "*\tchunkName = " + chunkName + ", x = " + x + ", inputFileName = " + inputFileName, config)
	
	if (!Files.exists(Paths.get(getRefFilePath(config))))
	{
		dbgLog("bwa/" + x, t0, "#\tReference file " + getRefFilePath(config) + " not found on this node!", config)
		return null
	}
	
	if (inputFileIsUnzipped)
	{
		val s = readWholeFile(inputFileName, config)
		new PrintWriter(fqFileName) {write(s); close}
		dbgLog("bwa/" + x, t0, "0a\tSaved the content of unzipped file " + inputFileName + " to local file " + fqFileName, config)
	}
	else
	{
		if (unzipBWAInputDirectly)
		{
			var inputFileBytes: Array[Byte] = null
			if (config.getMode != "local")
			{
				inputFileBytes = hdfsManager.readBytes(inputFileName)
				dbgLog("bwa/" + x, t0, "0a\tSize of zipped file = " + inputFileName + " in bytes = " + inputFileBytes.size, config)
			}
			else
			{
				val s = readWholeFile(inputFileName, config)
				inputFileBytes = s.getBytes
			}
			val decompressedStr = new GzipDecompressor(inputFileBytes).decompress
			new PrintWriter(fqFileName) {write(decompressedStr); close}
		}
		else
		{
			hdfsManager.download(x + ".gz", config.getInputFolder, config.getTmpFolder, false)
			val unzipStr = "gunzip -c " + inputFileName
			dbgLog("bwa/" + x, t0, "0a\t" + unzipStr + ", Input file size = " + ((new File(inputFileName).length) / (1024 * 1024)) + " MB. ", config)
			unzipStr #> new java.io.File(fqFileName) !;
			if (config.getMode != "local")
				new File(inputFileName).delete
		}
	}
	dbgLog("bwa/" + x, t0, "0b\tFastq file size = " + ((new File(fqFileName).length) / (1024 * 1024)) + " MB", config)
	// bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// Example: bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// run bwa mem
	val progName = getBinToolsDirPath(config) + "bwa mem "
	val command_str = progName + getRefFilePath(config) + " " + config.getExtraBWAParams + " -t " + config.getNumThreads + " " + fqFileName
	var writerMap = new scala.collection.mutable.HashMap[Long, StringBuilder]()
	
	dbgLog("bwa/" + x, t0, "1\tbwa mem started: " + command_str, config)
	val samRegionsParser = new SamRegionsParser(x, writerMap, config)
	val logger = ProcessLogger(
		(o: String) => {
			samRegionsParser.append(o)
			},
		(e: String) => {} // do nothing
	)
	command_str ! logger;
	
	dbgLog("bwa/" + x, t0, "2a\t" + "bwa (size = " + (new File(getBinToolsDirPath(config) + "bwa").length) + 
		") mem completed for %s -> Number of key value pairs = %d, reads = %d, badLines = %d".format(x, writerMap.size, 
			samRegionsParser.getNumOfReads, samRegionsParser.getBadLines), config)
	new File(fqFileName).delete()
	
	val parMap = writerMap.par
	val nThreads = config.getNumThreads.toInt
	parMap.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(nThreads))
	val sbDbg = new StringBuilder(4096)
	val arr = parMap.map{case (k, content) => 
		{
			val lines = content.split('\n')
			if (lines.size > 500)
			{
				val chrNum = k / SF
				val pos = k % SF
				val sb = new StringBuilder(1024 * 1024)
				val s = scala.collection.mutable.Set.empty[String]
				var reads = 0
				for(line <- lines)
				{
					val x = line.split('\t')
					val y = x(3) + x(4) + x(5) + x(6) + x(7)
					if (!s.contains(y))
					{
						s.add(y)
						sb.append(line + '\n')
						reads += 1
					}
				}
				sbDbg.append(s"($chrNum, $pos):\t" + lines.size + " to " + reads + "\n")
				(k, reads, new GzipCompressor(sb.toString).compress)
			}
			else
				//(k, content.split('\n').size, new GzipCompressor(content.toString).compress)
				(k, lines.size, new GzipCompressor(content.toString).compress)
		}
	}.toArray
	dbgLog("bwa/" + x, t0, "2b\tCompressed the data.\n" + sbDbg.toString, config)
	
	val retArr = new Array[(Long, Int)](arr.size)
	
	if (!mtFileWrite)
	{
		val bfWriter = new Array[BinaryFileWriter](config.getNumRegionsForLB.toInt)
		for(i <- 0 until config.getNumRegionsForLB.toInt)
			bfWriter(i) = new BinaryFileWriter(config.getTmpFolder + i + "_cmp-" + x)
		for(i <-0 until arr.size)
		{
			val chrIndex = (arr(i)._1 / SF).toInt
			val region = config.getChrRegion(chrIndex)
			bfWriter(region).writeRecord(arr(i))
			retArr(i) = (arr(i)._1, arr(i)._2)
		}
		for(i <- 0 until config.getNumRegionsForLB.toInt)
		{
			bfWriter(i).close
		
			if (config.getMode() != "local")
			{
				new File(config.getTmpFolder + "." + i + "_cmp-" + x + ".crc").delete
				hdfsManager.upload(i + "_cmp-" + x, config.getTmpFolder, config.getOutputFolder + "bwaOut/" + i + "/")
			}
		}
		dbgLog("bwa/" + x, t0, "3\t" + "Content uploaded to the HDFS", config)
	}
	else
	{
		val threadArray = new Array[Thread](nThreads)
		val indexes = new Array[(Int, Int)](nThreads)
		val elsPerThread = arr.size / nThreads
		val fileNames = new Array[String](nThreads)
		
		for(thread <- 0 until nThreads)
		{
			val si = thread * elsPerThread
			var ei = (thread + 1) * elsPerThread
			if (thread == (nThreads-1))
				ei = arr.size
			indexes(thread) = (si, ei)
		}
			
		for(thread <- 0 until nThreads)
		{
			threadArray(thread) = new Thread 
			{
				override def run 
				{
					fileNames(thread) = "cmp" + thread + "-" + x
					val bfWriter = new BinaryFileWriter(config.getTmpFolder + fileNames(thread))
					val i = indexes(thread)
					for(a <- i._1 until i._2)
					{
						bfWriter.writeRecord(arr(a))
						retArr(a) = (arr(a)._1, arr(a)._2)
					}
					bfWriter.close
					if (config.getMode() != "local")
					{
						new File(config.getTmpFolder + ".cmp" + thread + "-" + x + ".crc").delete
						hdfsManager.upload(fileNames(thread), config.getTmpFolder(), config.getOutputFolder + "bwaOut/")
					}
					fileNames(thread).synchronized
					{
						dbgLog("bwa/" + x, t0, "3b\t" + "File " + fileNames(thread) + " uploaded to the HDFS", config)
					}
				}
			}
			threadArray(thread).start
		}
			
		for(thread <- 0 until nThreads)
			threadArray(thread).join
	}
	
	retArr
}

def getSamRecords(x: String, lbRegion: Int, chrPosMap: scala.collection.Map[Long, Int], config: Configuration) : Array[(Int, Array[Byte])] = 
{
	val bfr = new BinaryFileReader
	val ab = scala.collection.mutable.ArrayBuffer.empty[(Int, Array[Byte])]
	val hdfsManager = new HDFSManager
	
	if ((writeToLog == true) && (config.getMode != "local"))
		hdfsManager.create(config.getOutputFolder + "log/gsr/" + lbRegion + "/" + x)
			
	val t0 = System.currentTimeMillis
	dbgLog("gsr/" + lbRegion + "/" + x, t0, "1\tDownloading...", config)
	if (config.getMode != "local")
	{
		if (readLBInputDirectly)
			bfr.setSource(hdfsManager.readBytes(config.getOutputFolder + "bwaOut/" + lbRegion + "/" + x))
		else
		{
			hdfsManager.download(x, config.getOutputFolder + "bwaOut/" + lbRegion + "/", config.getTmpFolder, false)
			bfr.read(config.getTmpFolder + x)
		}
	}
	else
		bfr.read(config.getTmpFolder + x)
	dbgLog("gsr/" + lbRegion + "/" + x, t0, "2\tCompleted reading input", config)

	var r = bfr.readRecord
	var count = 0
	while(r != null)
	{
		ab.append((chrPosMap(r._1), r._3))
		count += 1
		r = bfr.readRecord
	}
	bfr.close
	if ((config.getMode == "local") || !readLBInputDirectly) 
		new File(config.getTmpFolder + x).delete
	dbgLog("gsr/" + lbRegion + "/" + x, t0, "3\tDone. Total records = " + count, config)
	
	return ab.toArray
}

def uploadFileToOutput(filePath: String, outputPath: String, delSrc: Boolean, config: Configuration)
{
	try 
	{
		if (config.getMode() != "local")
		{
			val fileName = getFileNameFromPath(filePath)
			new File(config.getTmpFolder + "." + fileName + ".crc").delete()
			// Now upload
			val hconfig = new org.apache.hadoop.conf.Configuration()
			hconfig.addResource(new org.apache.hadoop.fs.Path(config.getHadoopInstall + "etc/hadoop/core-site.xml"))
			hconfig.addResource(new org.apache.hadoop.fs.Path(config.getHadoopInstall + "etc/hadoop/hdfs-site.xml"))
		
			val fs = org.apache.hadoop.fs.FileSystem.get(hconfig)
			fs.copyFromLocalFile(delSrc, true, new org.apache.hadoop.fs.Path(config.getTmpFolder + fileName), 
				new org.apache.hadoop.fs.Path(config.getOutputFolder + outputPath + "/" + fileName))
		}
	}
	catch 
	{
		case e: Exception => errLog(outputPath, 0, 
			"\tException in uploadFileToOutput: " + ExceptionUtils.getStackTrace(e) , config) 
	}
}

def buildRegion(index: Int, lbRegion: Int, lines: Array[Array[Byte]], config: Configuration) : (String, Int) =
{
	val region = getRegion(index, lbRegion, lines, config)
	return createBAMAndBEDFiles(index, lbRegion, region._2, config)
}
		
def getRegion(chrRegion: Integer, lbRegion: Integer, samRecordsZipped: Array[Array[Byte]], 
	config: Configuration) : (Int, Array[SAMRecord]) =
{
	val writeInString = true
	val hdfsManager = new HDFSManager
	
	var t0 = System.currentTimeMillis
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
		{
			if (hdfsManager.exists(config.getOutputFolder + "log/getRegion/" + lbRegion + "/region_" + chrRegion))
			{
				if (!hdfsManager.exists(config.getOutputFolder + "log/getRegion/fault/" + lbRegion + "/region_" + chrRegion))
					hdfsManager.create(config.getOutputFolder + "log/getRegion/fault/" + lbRegion + "/region_" + chrRegion)
				dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\t!!! Restarted by Spark !!!", config)
				errLog("getRegion:", t0, "Error encountered for " + lbRegion + "/region_" + chrRegion , config)
			}
			else
				hdfsManager.create(config.getOutputFolder + "log/getRegion/" + lbRegion + "/region_" + chrRegion)
		}
		if (downloadNeededFiles)
			downloadDictFile(config)
	}
	
	var samRecordsSorted: Array[SAMRecord] = null 
	if (writeInString)
	{
		val nThreads = 4
		val srecs = new Array[scala.collection.mutable.ArrayBuffer[SAMRecord]](nThreads)
		val threadArray = new Array[Thread](nThreads)
		val si = new Array[Int](nThreads)
		val ei = new Array[Int](nThreads)
		val totalCount = new Array[Int](nThreads)
		val badLines = new Array[Int](nThreads)
		val elsPerThread = samRecordsZipped.size / nThreads
		
		dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1a\tCreating key value pairs, elsPerThreads = " + 
			elsPerThread + ", total elements = " + samRecordsZipped.size, config)
		
		for(thread <- 0 until nThreads)
		{
			si(thread) = thread * elsPerThread
			if (thread == (nThreads-1))
				ei(thread) = samRecordsZipped.size
			else
				ei(thread) = (thread+1) * elsPerThread
		}
		//////////////////////////////////////////////////////////////////////
		for(thread <- 0 until nThreads)
		{
			threadArray(thread) = new Thread 
			{
				override def run
				{
					try{
					srecs(thread) = new scala.collection.mutable.ArrayBuffer[SAMRecord](256 * 1024) 
					
					var expectedSize = 8 * 1024 * 1024
					var sb = new StringBuilder(expectedSize)
					var count = 0
					val limit = 1000
					var printCounter = 0
					totalCount(thread) = 0
					badLines(thread) = 0
					sb.append(readDictFile(config))
					
					for (i <- si(thread) until ei(thread))
					{
						sb.append(new GzipDecompressor(samRecordsZipped(i)).decompress)
					
						count += 1
						totalCount(thread) += 1
						if (count == limit)
						{
							val bwaKeyValues = new BWAKeyValuesString(new StringBufferInputStream(sb.toString), chrRegion, config)
							bwaKeyValues.setSamRecs(srecs(thread))
							badLines(thread) += bwaKeyValues.parseSam
							bwaKeyValues.close
							
							printCounter += 1
							if ((printCounter % 1000) == 0)
							{
								srecs(thread).synchronized
								{
									dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\t" + printCounter + "." + count + 
										"recs processed!, sb.size = " + sb.size, config)
								}
							}
							expectedSize = (sb.size * 1.5).toInt
							
							sb = new StringBuilder(readDictFile(config))
							count = 0
						}
					}
					val bwaKeyValues = new BWAKeyValuesString(new StringBufferInputStream(sb.toString), chrRegion, config)
					bwaKeyValues.setSamRecs(srecs(thread))
					badLines(thread) += bwaKeyValues.parseSam
					bwaKeyValues.close
					} catch 
					{
						case e: Exception =>
						{
							dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "*\tError\n" + ExceptionUtils.getStackTrace(e), config)
						}
					}
				}
			}
			threadArray(thread).start
		}
		
		for(thread <- 0 until nThreads)
			threadArray(thread).join
		//////////////////////////////////////////////////////////////////////	
		var srecsCombined = srecs(0)
		var totalCountCombined = totalCount(0)
		var badLinesCombined = badLines(0)
		if (nThreads > 1)
		{
			for(i <- 1 until nThreads)
			{
				srecsCombined ++= srecs(i)
				totalCountCombined += totalCount(i)
				badLinesCombined += badLines(i)
			}
		}
		samRecordsSorted = srecsCombined.toArray
		dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of records = " + 
			totalCountCombined + ". Number of bad lines = " + badLinesCombined, config)
	}
	else
	{
		dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1a\tCreating key value pairs", config)
		val fileName = config.getTmpFolder + lbRegion + "_" + chrRegion + ".bin"
		val writer = new BufferedWriter(new FileWriter(fileName))
		writer.write(readDictFile(config))
		var count = 0
		for (sr <- samRecordsZipped)
		{
			writer.write(new GzipDecompressor(sr).decompress)
			count += 1
		}
		writer.close
		dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\tNumber of records = " + count, config)
		val bwaKeyValues = new BWAKeyValues(fileName, chrRegion, config)
		val badLines = bwaKeyValues.parseSam
		samRecordsSorted = bwaKeyValues.getArray
		bwaKeyValues.close
		new File(fileName).delete
		dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of bad lines = " + badLines, config)
	}
	
	// Sorting
	implicit val samRecordOrdering = new Ordering[SAMRecord] {
		override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
	}
	scala.util.Sorting.quickSort(samRecordsSorted)
	//
	
	dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "3\t" + samRecordsSorted.size + " reads sorted!", config)
	return (chrRegion, samRecordsSorted)
}

def createBAMAndBEDFiles(chrRegion: Integer, lbRegion: Integer, samRecordsSorted: Array[SAMRecord], config: Configuration) : (String, Int) = 
{	
	val tmpFileBase = config.getTmpFolder + lbRegion + "_" + chrRegion
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val hdfsManager = new HDFSManager
	
	if ((writeToLog == true) && (config.getMode != "local"))
		hdfsManager.create(config.getOutputFolder + "log/bam/" + lbRegion + "/region_" + chrRegion.toString)
	
	var t0 = System.currentTimeMillis
	
	dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "2\tVC creating BAM file. Number of sam records = " + 
		samRecordsSorted.size, config)
	
	var count = 0
	val r = new ChromosomeRange()
	var sam: SAMRecord = null 
	var badLines = 0
	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	val outHeader = header.clone()
	//////////////////////////////
	val bamrg = new SAMReadGroupRecord("GROUP1")
	bamrg.setLibrary("LIB1")
	bamrg.setPlatform("ILLUMINA")
	bamrg.setPlatformUnit("UNIT1")
	bamrg.setSample("SAMPLE1")
	outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
	outHeader.addReadGroup(bamrg)
	//////////////////////////////
	val factory = new SAMFileWriterFactory();
	val writer = factory.makeBAMWriter(outHeader, true, new File(tmpOut1));
	
	val RGID = "GROUP1"
	val input = new tudelft.utils.SAMRecordIterator(samRecordsSorted, header, r)
	while(input.hasNext()) 
	{
		try
		{
			sam = input.next()
			/////////////////////////////////////////
			sam.setAttribute(SAMTag.RG.name(), RGID)
			/////////////////////////////////////////
			writer.addAlignment(sam)
			count += 1
		}
		catch 
		{
			case e: Exception =>  
			{ 
				dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString, t0, "*\tError\nSam =  " + sam, config)
				statusLog("Create BAM error:", t0, "Encountered error in a line for region " + chrRegion, config)
				badLines += 1
			}
		}
	}
	input.addLastChrRange()
	val reads = input.getCount()
	writer.close()
	
	dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "3\tMaking region file. There are " + count + " reads in total.", config)
	var region: String = null 
	makeRegionFile(tmpFileBase, r, config)
	dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "4\tDone making the region file.", config)
	
	if (saveAllStages)
		uploadFileToOutput(tmpOut1, "bamOutput", false, config)
		
	uploadFileToOutput(tmpOut1, "bam", true, config)
	uploadFileToOutput(config.getTmpFolder + lbRegion + "_" + chrRegion + ".bed", "bed", true, config)
	
	dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "4\tUploaded bam and bed files to the output.", config)
	
	if (config.getMode != "local")
	{ 
		new File(tmpOut1).delete
		new File(tmpFileBase + ".bed").delete
	}
	
	return (chrRegion.toString, samRecordsSorted.size)
}

def copyExomeBed(exomeBed: String, config: Configuration)
{
	val lines = readWholeFile(config.getExomePath, config)
	var from = 0
		
	if (lines(0) == '@')
	{
		var done = false
		
		while(!done)
		{
			var next = lines.indexOf('\n', from)
			from = next + 1
			if (lines(from) != '@')
				done = true
		}
	}
		
	new PrintWriter(exomeBed) {write(lines.substring(from)); close}
}

def makeCorrectBedFile(cmdStr: String, bedFile: String)
{
	val cmdRes = (cmdStr #> new java.io.File(bedFile + ".1")).!
	val lines = scala.io.Source.fromFile(bedFile + ".1").mkString.split('\n')
	var s = ""
	
	for( line <- lines)
	{
		val e = line.split('\t')
		val lval = e(1).toLong
		var hval = e(2).toLong
		
		if (lval == hval)
			hval += 1
		
		s += e(0) + "\t" + lval + "\t" + hval + "\n"
	}
	
	new PrintWriter(bedFile) {write(s); close}
	new File(bedFile + ".1").delete()
}

def makeRegionFile(tmpFileBase: String, r: ChromosomeRange, config: Configuration)
{
	val bedFile = tmpFileBase + ".bed"
	val hdfsManager = new HDFSManager
	
	if (config.useExome())
	{
		val toolsFolder = getBinToolsDirPath(config)
		val exomeBed = tmpFileBase + "_exome.bed"
		
		copyExomeBed(exomeBed, config)
		
		if (config.getMode != "local")
		{
			hdfsManager.downloadIfRequired("bedtools", config.getToolsFolder, config.getTmpFolder)
			val file = new File(config.getTmpFolder + "bedtools") 
			file.setExecutable(true)
		}
		
		 // write a bed file with the region!
		val bed = new File(exomeBed + "_tmp.bed")
		r.writeToBedRegionFile(bed.getAbsolutePath())
		makeCorrectBedFile(toolsFolder + "bedtools intersect -a " + exomeBed + " -b " + bed + " -header", bedFile)
		
		// Delete temporary files
		new File(exomeBed).delete()
		new File(exomeBed + "_tmp.bed").delete()
	}
	else
	{
		val bed = new File(bedFile)
		r.writeToBedRegionFile(bed.getAbsolutePath())
	}
}

def createSAMWriter(fileName: String, config: Configuration) : SAMFileWriter =
{
	val header = createHeader(config)
	val factory = new SAMFileWriterFactory()
	return factory.makeSAMWriter(header, false, new File(fileName))
}

def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
{
	if(a.getReferenceIndex == b.getReferenceIndex)
		return a.getAlignmentStart - b.getAlignmentStart
	else
		return a.getReferenceIndex - b.getReferenceIndex
}

def createHeader(config: Configuration) : SAMFileHeader =
{
	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	
	return header
}

def makeDirIfRequired(dir: String, config: Configuration)
{
	if (config.getMode == "local")
	{
		val file = new File(dir)
		if (!file.exists())
			file.mkdir()
	}			
}

def variantCall(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
{
	val tmpFileBase = config.getTmpFolder + chrRegion
	var t0 = System.currentTimeMillis
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
			hdfsManager.create(config.getOutputFolder + "log/vc/" + "region_" + chrRegion)
		
		if (!(new File(config.getTmpFolder).exists))
			new File(config.getTmpFolder).mkdirs()
		
		hdfsManager.download(chrRegion + "-p1.bam", config.getOutputFolder + "bam/", config.getTmpFolder, false)
		hdfsManager.download(chrRegion + ".bed", config.getOutputFolder + "bed/", config.getTmpFolder, false)
		dbgLog("vc/region_" + chrRegion, t0, "1\tDownloaded bam and bed files to the local directory!", config)
	}

	val f = new File(tmpFileBase + ".bed");
	if (!(f.exists() && !f.isDirectory())) 
		dbgLog("vc/region_" + chrRegion, t0, "#-\tbed file does not exist in the tmp directory!!", config)
	
	if ((config.getMode != "local") && downloadNeededFiles)
	{
		dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading vcf tools", config)
		downloadVCFTools(config)
	}
	
	try 
	{
		picardPreprocess(tmpFileBase, config)
		if ((config.getMode != "local") && downloadNeededFiles)
		{
			dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading ref files for variant calling", config)
			downloadVCFRefFiles("dlvcf/region_" + chrRegion, config)
		}
		if (doIndelRealignment)
			indelRealignment(tmpFileBase, t0, chrRegion, config)
		baseQualityScoreRecalibration(tmpFileBase, t0, chrRegion, config)
		dnaVariantCalling(tmpFileBase, t0, chrRegion, config)
		
		val retArray = getVCF(chrRegion, config)
		
		if (config.getMode != "local")
		{
			new File(config.getTmpFolder + "." + chrRegion + ".vcf.crc").delete
			uploadFileToOutput(config.getTmpFolder + chrRegion + ".vcf", "vcOut", true, config)
		}
		
		dbgLog("vc/region_" + chrRegion, t0, "vcf\tOutput written to vcf file", config)
		return retArray
	} 
	catch 
	{
		case e: Exception => {
			dbgLog("vc/region_" + chrRegion, t0, "exception\tAn exception occurred: " + ExceptionUtils.getStackTrace(e), config)
			statusLog("Variant call error:", t0, "Variant calling failed for " + chrRegion, config)
			return null
		}
	}
}

def picardPreprocess(tmpFileBase: String, config: Configuration)
{
	val toolsFolder = getToolsDirPath(config)
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val tmpOut2 = tmpFileBase + "-p2.bam"
	val MemString = config.getExecMemX()
	val chrRegion = getFileNameFromPath(tmpFileBase)
	val logFileName = "vc/region_" + chrRegion
	
	var t0 = System.currentTimeMillis
	
	dbgLog(logFileName, t0, "picard\tPicard processing started", config)
	
	try
	{
		var cmdStr = "java " + MemString + " -jar " + toolsFolder + "CleanSam.jar INPUT=" + tmpOut1 + " OUTPUT=" + tmpOut2
		cmdStr.!!
		
		val bamOut = tmpFileBase + ".bam"
		val tmpMetrics = tmpFileBase + "-metrics.txt"
		
		cmdStr = "java " + MemString + " -jar " + toolsFolder + "MarkDuplicates.jar INPUT=" + tmpOut2 + " OUTPUT=" + bamOut +
			" METRICS_FILE=" + tmpMetrics + " CREATE_INDEX=true";
		cmdStr.!!
		
		// Hamid - Save output of picardPreprocessing
		if (saveAllStages)
			uploadFileToOutput(bamOut, "picardOutput", false, config)
		
		// Delete temporary files
		dbgLog(logFileName, t0, "picard\tDeleting files " + tmpOut1 + ", " + tmpOut2 + ", and " + tmpMetrics, config)
		new File(tmpMetrics).delete
	}
	catch 
	{
		case e: Exception => {
			dbgLog(logFileName, t0, "exception\tAn exception occurred: " + ExceptionUtils.getStackTrace(e), config)
			statusLog("Picard error:", t0, "Picard failed for " + chrRegion, config)
		}
	}
	
	new File(tmpOut1).delete
	new File(tmpOut2).delete
}

def indelRealignment(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) =
{
	val toolsFolder = getToolsDirPath(config)
	val tmpFile1 = tmpFileBase + "-2.bam"
	val preprocess = tmpFileBase + ".bam"
	val targets = tmpFileBase + ".intervals"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = "" 
	
	// Realigner target creator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T RealignerTargetCreator -nt " + 
	config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + preprocess + indelStr + " -o " +
		targets + regionStr
	dbgLog("vc/region_" + chrRegion, t0, "indel1\t" + cmdStr, config)
	cmdStr.!!
	
	// Indel realigner
	cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T IndelRealigner -R " + 
		getRefFilePath(config) + " -I " + preprocess + " -targetIntervals " + targets + indelStr + " -o " + tmpFile1 + regionStr
	dbgLog("vc/region_" + chrRegion, t0, "indel2\t" + cmdStr, config)
	cmdStr.!!
	
	// Hamid - Save output of indelRealignment
	if (saveAllStages)
		uploadFileToOutput(tmpFile1, "indelOutput", false, config)
	
	// Delete temporary files
	dbgLog("vc/region_" + chrRegion, t0, "indel3\tDeleting files " + preprocess + " and " + targets, config)
	new File(preprocess).delete
	new File(preprocess.replace(".bam", ".bai")).delete
	new File(targets).delete
}
	
def baseQualityScoreRecalibration(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration)
{
	val toolsFolder = getToolsDirPath(config)
	val knownSite = getSnpFilePath(config)
	val tmpFile1 = if (doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
	val tmpFile2 = tmpFileBase + "-3.bam"
	val table = tmpFileBase + ".table"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = "" 
	
	// Base recalibrator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T BaseRecalibrator -nct " + 
		config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile1 + " -o " + table + regionStr + 
		" --disable_auto_index_creation_and_locking_when_reading_rods" + indelStr + " -knownSites " + knownSite
	dbgLog("vc/region_" + chrRegion, t0, "base1\t" + cmdStr, config)
	cmdStr.!!

	if (doPrintReads)
	{
		// Print reads
		cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T PrintReads -R " + 
			getRefFilePath(config) + " -I " + tmpFile1 + " -o " + tmpFile2 + " -BQSR " + table + regionStr 
		dbgLog("vc/region_" + chrRegion, t0, "base2\t" + cmdStr, config)
		cmdStr.!!
	
		// Hamid - Save output of baseQualityScoreRecalibration
		if (saveAllStages)
			uploadFileToOutput(tmpFile2, "baseOutput", false, config)
	
		// Delete temporary files
		dbgLog("vc/region_" + chrRegion, t0, "base3(doPrintReads)\tDeleting files " + tmpFile1 + " and " + table, config)
		new File(tmpFile1).delete
		new File(tmpFile1.replace(".bam", ".bai")).delete
		new File(table).delete
	}
}

def dnaVariantCalling(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration)
{
	val toolsFolder = getToolsDirPath(config)
	val tmpFile2 = if (doPrintReads) (tmpFileBase + "-3.bam") else if (doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
	val snps = tmpFileBase + ".vcf"
	val bqsrStr = if (doPrintReads) "" else (" -BQSR " + tmpFileBase + ".table ")
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	
	// Haplotype caller
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T HaplotypeCaller -nct " + 
		config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile2 + bqsrStr + " --genotyping_mode DISCOVERY -o " + snps + 
		" -stand_call_conf " + config.getSCC() + " -stand_emit_conf " + config.getSEC() + regionStr + 
		" --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods"
	dbgLog("vc/region_" + chrRegion, t0, "haplo1\t" + cmdStr, config)
	cmdStr.!!
	
	// Delete temporary files
	dbgLog("vc/region_" + chrRegion, t0, "haplo2\tDeleting files " + tmpFile2 + ", " + (tmpFileBase + ".bed") +
		(if (!doPrintReads) (" and " + tmpFileBase + ".table") else "."), config)
	new File(tmpFile2).delete()
	new File(tmpFile2.replace(".bam", ".bai")).delete
	new File(tmpFileBase + ".bed").delete
	if (!doPrintReads)
		new File(tmpFileBase + ".table").delete
}
	
def getVCF(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
{
	var a = scala.collection.mutable.ArrayBuffer.empty[((Integer, Integer), String)]
	var fileName = config.getTmpFolder() + chrRegion + ".vcf"
	var commentPos = 0
	
	if (!Files.exists(Paths.get(fileName)))
		return a.toArray
	
	for (line <- Source.fromFile(fileName).getLines()) 
	{
		val c = line(0)
		if (c != '#')
		{
			val e = line.split('\t')
			val position = e(1).toInt
			var chromosome = e(0)
			var chrNumber = 0
			
			if (e(0).contains("chr"))
				chromosome = e(0).substring(3)
			
			try{chrNumber = chromosome.toInt}
			catch{case _: Throwable => chrNumber = if (chromosome.contains('X')) 101 else 102;}
				
			a.append(((chrNumber, position), line))
		}
		else
		{
			a.append(((-1, commentPos), line))
			commentPos = commentPos + 1
		}
	}
	
	// Delete temporary file
	new File(fileName + ".idx").delete()
	
	return a.toArray
}
	
def getInputFileNames(dir: String, config: Configuration) : Array[String] = 
{
	val mode = config.getMode
	val hdfsManager = new HDFSManager
	
	if (mode != "local")
	{
		val a: Array[String] = hdfsManager.getFileList(dir)

		return a
	}
	else
	{
		var d = new File(dir)	
		
		if (d.exists && d.isDirectory) 
		{
			val list: List[File] = d.listFiles.filter(_.isFile).toList
			val a: Array[String] = new Array[String](list.size)
			var i = 0
			
			for(i <- 0 until list.size)
				a(i) = list(i).getName
			
			return a
		} 
		else
			return null
	}
}

def getFileNameFromPath(path: String) : String =
{
	return path.substring(path.lastIndexOf('/') + 1)
}

def getDirFromPath(path: String) : String =
{
	return path.substring(0, path.lastIndexOf('/') + 1)
}

def getRefFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getRefPath() else 
		config.getSfFolder() + getFileNameFromPath(config.getRefPath())
}

def getSnpFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getSnpPath() else
		config.getSfFolder() + getFileNameFromPath(config.getSnpPath())
}

def getDictFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getDictPath() else 
		config.getSfFolder() + getFileNameFromPath(config.getDictPath())
}

def getIndelFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getIndelPath() else
		config.getSfFolder() + getFileNameFromPath(config.getIndelPath())
}

def getExomeFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getExomePath() else
		config.getSfFolder() + getFileNameFromPath(config.getExomePath())
}

def getToolsDirPath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getToolsFolder() else config.getSfFolder()
}

def getBinToolsDirPath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getToolsFolder() else config.getTmpFolder()
}

def getTimeStamp() : String =
{
	return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
}

def log(fname: String, key: String, t0: Long, message: String, config: Configuration) = 
{
	val ct = System.currentTimeMillis
	val at = (ct - config.getStartTime()) / 1000
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
	{
		val IP = InetAddress.getLocalHost().toString()
		val node = IP.substring(0, IP.indexOf('/'))
		// Node, time, absolute time, key, message
		hdfsManager.append(fname, node + "\t" + getTimeStamp() + "\t" + 
			at.toString() + "\t" + message + "\n")
	}
	else
	{
		val s = getTimeStamp() + "\t" + at.toString() + "\t" + message
		println(s)
		val fw = new FileWriter(fname, true) 
		fw.write(s + "\n") 
		fw.close()
	}
}

def statusLog(key: String, t0: Long, message: String, config: Configuration) =
{
	log("sparkLog.txt", key, t0, key + "\t" + message, config)
}

def dbgLog(key: String, t0: Long, message: String, config: Configuration) =
{
	if (writeToLog == true)
		log(config.getOutputFolder + "log/" + key, key, t0, message, config)
}

def errLog(key: String, t0: Long, message: String, config: Configuration) =
{
	log("errorLog.txt", key, t0, key + "\t" + message, config)
}

def readWholeFile(fname: String, config: Configuration) : String =
{
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
		return hdfsManager.readWholeFile(fname)
	else
		return new String(Files.readAllBytes(Paths.get(fname)))
		//return scala.io.Source.fromFile(fname).mkString
}

def readDictFile(config: Configuration) : String =
{
	return scala.io.Source.fromFile(getDictFilePath(config)).mkString
}

def readPartialFile(fname: String, bytes: Int, config: Configuration) : String =
{
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
		return hdfsManager.readPartialFile(fname, bytes)
	else
		return scala.io.Source.fromFile(fname).mkString
}

def writeWholeFile(fname: String, s: String, config: Configuration)
{
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
		hdfsManager.writeWholeFile(fname, s)
	else
		new PrintWriter(fname) {write(s); close}
}

def gunZipDownloadedFile(x: String, filePath: String, config: Configuration) : Long =
{
	val fileName = getFileNameFromPath(filePath)
	val hdfsManager = new HDFSManager
	val fileSize = hdfsManager.getFileSize(filePath)
	
	dbgLog(x, 0, "#1\tfilePath = " + filePath + ", fileSize = " + fileSize, config)
	try{("gunzip " + config.getTmpFolder + fileName + ".gz").!}
	catch{case e: Exception => dbgLog(x, 0, "#gunzip\nEither already unzipped or some other thread is unzipping it!", config)}
	val f = new File(config.getTmpFolder + fileName)
	@volatile var flen = f.length
	
	var iter = 0
	while(flen != fileSize)
	{
		if ((iter % 10) == 0)
			dbgLog(x, 0, "#2\tflen = " + flen + ", fileSize = " + fileSize, config)
		iter += 1
		Thread.sleep(1000)
		flen = f.length
	}
	dbgLog(x, 0, "#3\tflen = " + flen + ", fileSize = " + fileSize, config)
	
	return flen
}

def fileToDownloadAlreadyExists(hdfsPath: String, config: Configuration) : Boolean =
{
	val fileName = getFileNameFromPath(hdfsPath)
	val hdfsManager = new HDFSManager
	val fileSize = hdfsManager.getFileSize(hdfsPath)
	val f = new File(config.getSfFolder + fileName)
	
	return f.exists && (f.length == fileSize)
}

def downloadBWAFiles(x: String, config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	val hdfsManager = new HDFSManager
	
	if (!(new File(config.getSfFolder).exists))
		new File(config.getSfFolder()).mkdirs()
	
	if (!fileToDownloadAlreadyExists(config.getRefPath, config))
	{
		hdfsManager.downloadIfRequired(refFileName + ".gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath, config)
	}
	hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	hdfsManager.downloadIfRequired(refFileName + ".amb", refFolder, config.getSfFolder)
	hdfsManager.downloadIfRequired(refFileName + ".ann", refFolder, config.getSfFolder)
	if (!fileToDownloadAlreadyExists(config.getRefPath + ".bwt", config))
	{
		hdfsManager.downloadIfRequired(refFileName + ".bwt.gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath + ".bwt", config)
	}
	hdfsManager.downloadIfRequired(refFileName + ".fai", refFolder, config.getSfFolder)
	hdfsManager.downloadIfRequired(refFileName + ".pac", refFolder, config.getSfFolder)
	hdfsManager.downloadIfRequired(refFileName + ".sa", refFolder, config.getSfFolder)
}

def downloadVCFTools(config: Configuration)
{
	val hdfsManager = new HDFSManager
	
	hdfsManager.downloadIfRequired("AddOrReplaceReadGroups.jar", config.getToolsFolder(), config.getSfFolder())
	hdfsManager.downloadIfRequired("BuildBamIndex.jar", config.getToolsFolder(), config.getSfFolder())
	hdfsManager.downloadIfRequired("CleanSam.jar", config.getToolsFolder(), config.getSfFolder())
	hdfsManager.downloadIfRequired("GenomeAnalysisTK.jar", config.getToolsFolder(), config.getSfFolder())
	hdfsManager.downloadIfRequired("MarkDuplicates.jar", config.getToolsFolder(), config.getSfFolder())
}

def downloadVCFRefFiles(x: String, config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	val hdfsManager = new HDFSManager
	
	if (!(new File(config.getSfFolder).exists))
		new File(config.getSfFolder()).mkdirs()
	
	if (!fileToDownloadAlreadyExists(config.getRefPath, config))
	{
		hdfsManager.downloadIfRequired(refFileName + ".gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath, config)
	}
	hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	hdfsManager.downloadIfRequired(refFileName + ".fai", refFolder, config.getSfFolder)
	
	if (config.getUseKnownIndels != "false")
	{
		val indelFolder = getDirFromPath(config.getIndelPath())
		val indelFileName = getFileNameFromPath(config.getIndelPath())
		hdfsManager.downloadIfRequired(indelFileName, indelFolder, config.getSfFolder())
		hdfsManager.download(indelFileName + ".idx", indelFolder, config.getSfFolder, true)
	}
}

def downloadVCFSnpFile(x: String, config: Configuration)
{
	val snpFolder = getDirFromPath(config.getSnpPath)
	val snpFileName = getFileNameFromPath(config.getSnpPath)
	val hdfsManager = new HDFSManager
	
	if (!fileToDownloadAlreadyExists(config.getSnpPath, config))
	{
		hdfsManager.downloadIfRequired(snpFileName + ".gz", snpFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getSnpPath, config)
	}
	hdfsManager.download(snpFileName + ".idx", snpFolder, config.getSfFolder, true)	
}

def downloadDictFile(config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	val hdfsManager = new HDFSManager
	
	hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
}

def downloadBinProgram(fileName: String, config: Configuration)
{	
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
		hdfsManager.downloadIfRequired(fileName, config.getToolsFolder(), config.getTmpFolder)
}

def writeRegionsMap(regionsMap: scala.collection.mutable.HashMap[Long, Int], config: Configuration)
{
	val sb = new StringBuilder
	for ((k,v) <- regionsMap)
		sb.append(k + "\t" + v + "\n")
	writeWholeFile(config.getOutputFolder + "regionsMap.txt", sb.toString, config)
}

def readRegionsMap(regionsMap: scala.collection.mutable.HashMap[Long, Int], config: Configuration)
{
	val content = readWholeFile(config.getOutputFolder + "regionsMap.txt", config)
	val lines = content.split('\n')
	for (e <- lines)
	{
		val x = e.split('\t')
		regionsMap(x(0).toLong) = x(1).toInt
	}
}

def getBamFileSize(fileID: String, config: Configuration) : Long =
{
	val hdfsManager = new HDFSManager
	return hdfsManager.getFileSize(config.getOutputFolder + "bam/" + fileID + "-p1.bam")
}

def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize(args(0), args(1))
	val part = args(1).toInt
	val part2Region = if (part == 2) args(2).toInt else 0
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	
	if (config.getMode == "local")
	{
		conf.setMaster("local[" + config.getNumInstances() + "]")
		conf.set("spark.cores.max", config.getNumInstances())
	}
	else
	{
		conf.set("spark.shuffle.blockTransferService", "nio") 
		if (compressRDDs)
			conf.set("spark.rdd.compress","true")
		conf.set("spark.network.timeout", "12000")
		if (part == 2)
		{
			//conf.set("spark.memory.fraction", "0.85")
			//conf.set("spark.memory.storageFraction", "0.75")
			conf.set("spark.memory.fraction", "0.7")
			conf.set("spark.memory.storageFraction", "0.7")
		}
	}
	conf.set("spark.driver.maxResultSize", "4g")
   
	val sc = new SparkContext(conf)
	val bcConfig = sc.broadcast(config)
	val hdfsManager = new HDFSManager
	
	// Comment these two lines if you want to see more verbose messages from Spark
	//Logger.getLogger("org").setLevel(Level.OFF);
	//Logger.getLogger("akka").setLevel(Level.OFF);
	
	config.print() 
	/////////////////////////////////////////////////////////
	/*val a = new Array[(Int, Int)](25)
	for(i <- 0 until 25)
		a(i) = (config.getChrRegion(i), i)
	// (region, <chr1, 2, ...>)
	val regions = sc.parallelize(a).groupByKey.collect
	
	for (reg <- regions)
	{
		var sum = 0
		val r = reg._1
		val elements = reg._2
		for(e <- elements)
		{
			println("chr " + e + " of len " + config.getChrLen(e) + " is in region " + r)
			sum += config.getChrLen(e)
		}
		println(">> sum of region " + r + " = " + (sum / 1e6) + " million")
	}
	println("total len of all chromosomes = " + (config.getChrLenSum / 1e6) + " million")
	println("total number of lb regions = " + config.getNumRegionsForLB)
	System.exit(1)*/
	/////////////////////////////////////////////////////////
	
	if (config.getMode != "local")
	{
		if (!hdfsManager.exists("sparkLog.txt"))
			hdfsManager.create("sparkLog.txt")
		if (!hdfsManager.exists("errorLog.txt"))
			hdfsManager.create("errorLog.txt")
	}
	else
	{
		val file = new File(config.getOutputFolder + "log")
		if ((writeToLog == true) && !file.exists())
			file.mkdir()
	}
	
	var t0 = System.currentTimeMillis
	val numOfRegions = config.getNumRegions.toInt
	// Spark Listener
	sc.addSparkListener(new SparkListener() 
	{
		override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
		{
			statusLog("SparkListener:", t0, getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n", config)
		}

		override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
		{
			statusLog("SparkListener:", t0, getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n", config)
		}

		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
		{
			val map = stageCompleted.stageInfo.rddInfos
			map.foreach(row => {
				if (row.isCached)
				{	
					statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + ": memSize = " + (row.memSize / (1024*1024)) + 
							"MB, diskSize " + row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions, config)
				}
				else if (row.name.contains("rdd_"))
				{
					statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + " processed!", config)
				}
			})
		}
	});
	//////////////////////////////////////////////////////////////////////////
	if (part == 1)
	{
		val inputFileNames = getInputFileNames(config.getInputFolder, config).filter(x => x.contains(".fq"))  
		if (inputFileNames == null)
		{
			println("The input directory " + config.getInputFolder() + " does not exist!")
			System.exit(1)
		}
		inputFileNames.foreach(println)
	
		// Give chunks to bwa instances
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size) 
		
		// Run instances of bwa and get the output as Key Value pairs
		// <Chr&Pos, SRLine>
		val bwaOut = inputData.flatMap(x => bwaRun(x, bcConfig.value))
		bwaOut.setName("rdd_bwaOut")
		bwaOut.persist(MEMORY_AND_DISK_SER)
		val bwaOutStr = bwaOut.map(x => x._1 + ":" + x._2)
		bwaOutStr.setName("rdd_bwaOutStr")
		bwaOutStr.saveAsTextFile(config.getOutputFolder + "chrPositions")
	}
	else if (part == 2)
	{	
		val regionsMap = new scala.collection.mutable.HashMap[Long, Int]
		if (part2Region == 0)
		{
			implicit val samRecordOrdering = new Ordering[SAMRecord] {
					override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
			}
			
			statusLog("chrPos: ", t0, "downloading and sorting", config)
			val chrPositionsText = sc.textFile(config.getOutputFolder + "chrPositions/part-*")
			val chrPositionsLong = chrPositionsText.map{x => val a = x.split(':'); (a(0).toLong, a(1).toInt)}.reduceByKey(_+_).sortByKey(true)
			chrPositionsLong.persist(MEMORY_AND_DISK_SER)
			chrPositionsLong.setName("rdd_chrPositionsLong")
			val elsPerRegion = chrPositionsLong.map(x => x._2).reduce(_+_) / config.getNumRegions.toInt
		
			var count = 0
			var curRegion = 0
			var numOfRegions = 1
			statusLog("chrPos: ", t0, "Making regions, elsPerRegion = " + elsPerRegion, config)
			for (e <- chrPositionsLong.collect)
			{
				val chrPos = e._1
				val nReads = e._2
				val chrNum = chrPos / SF
				val pos = chrPos % SF
				
				count += nReads
				if (nReads > elsPerRegion)
					statusLog("chrPos: ", t0, s"($chrNum, $pos) -> $nReads = ", config)
				if (count > elsPerRegion)
				{
					count = nReads
					curRegion += 1
					numOfRegions += 1
				}
				regionsMap(chrPos) = curRegion
			}
			if (config.getNumRegionsForLB.toInt > 1)
			{
				statusLog("chrPos: ", t0, "Writing regionsMap", config)
				writeRegionsMap(regionsMap, config)
			}
			statusLog("NumOfRegions:", t0, numOfRegions.toString, config)
		}
		else
		{
			statusLog("readRegionsMap: ", t0, "reading for region " + part2Region + "...", config)
			readRegionsMap(regionsMap, config)
			statusLog("readRegionsMap: ", t0, "done reading for region " + part2Region + ".", config)
		}	
		val bcChrPosMap = sc.broadcast(regionsMap)
		val i = part2Region
		
		val inputFileNames = getInputFileNames(if (config.getMode != "local") (config.getOutputFolder + "bwaOut/" + i) else config.getTmpFolder, config)
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		// RDD[(Int, Array[Byte])]
		val chrToSamRecord1 = inputData.flatMap(x => getSamRecords(x, i, bcChrPosMap.value, bcConfig.value))
		chrToSamRecord1.setName("rdd_chrToSamRecord1_" + i)
		chrToSamRecord1.persist(MEMORY_AND_DISK_SER)
		val chrToSamRecord2 = chrToSamRecord1.mapValues(ab => Array(ab)).reduceByKey((a1, a2) => a1 ++ a2)
		chrToSamRecord2.setName("rdd_chrToSamRecord2_" + i)
	
		val rdd = chrToSamRecord2.foreach(x => buildRegion(x._1, i, x._2, bcConfig.value))		
	}
	else // (part == 3)
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		val combinedVCFPath = config.getOutputFolder + "combinedVCF"
		if (hdfsManager.exists(combinedVCFPath))
			hdfsManager.remove(combinedVCFPath)
		
		var inputFileNames: Array[String] = null
		if (config.getMode != "local") 
			inputFileNames = getInputFileNames(config.getOutputFolder + "bed/", config).map(x => x.replace(".bed", ""))
		else 
			inputFileNames = getInputFileNames(config.getTmpFolder, config).filter(x => x.contains(".bed")).map(x => x.replace(".bed", ""))
		
		val inputData1 = sc.parallelize(inputFileNames, inputFileNames.size)
		val inputData1BySize = inputData1.map(x => (getBamFileSize(x, bcConfig.value), x))
		val inputData = inputData1BySize.sortByKey(false).map(_._2)
		inputData.setName("rdd_inputData")
		val vcf = inputData.flatMap(x => variantCall(x, bcConfig.value))
		vcf.setName("rdd_vc")
		vcf.distinct.sortByKey().map(_._2).coalesce(1, true).saveAsTextFile(combinedVCFPath)
	}
	//////////////////////////////////////////////////////////////////////////
	var et = (System.currentTimeMillis - t0) / 1000
	statusLog("Execution time:", t0, et.toString() + "\tsecs", config)
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
