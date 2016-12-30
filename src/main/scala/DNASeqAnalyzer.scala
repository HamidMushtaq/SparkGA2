/* SparkGA
 *
 * This file is a part of SparkGA.
 * 
 * Copyright (c) 2016-2017 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * SparkGA is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkGA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with SparkGA.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.util.SizeEstimator
import sys.process._

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.nio.file.Files
import java.nio.file.Paths
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Arrays

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.util.Sorting._

import htsjdk.samtools._

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator
import tudelft.utils.HDFSManager
import utils._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import collection.mutable.HashMap
import org.apache.spark.HashPartitioner

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParMap

import org.apache.spark.RangePartitioner

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
final val unzipBWAInputDirectly = true
final val readLBInputDirectly = true
// Pipeline stages
final val doIndelRealignment = false
final val doPrintReads = false
final val markDupWithLB = false

final val SF = 1e12.toLong
final val chrPosGran = 10000

val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkListener.txt"), "UTF-8"))
//////////////////////////////////////////////////////////////////////////////
def bwaRun (x: String, config: Configuration) : Array[(Long, Int)] = 
{
	var inputFileName = if ((config.getMode != "local") && !unzipBWAInputDirectly) (config.getTmpFolder + x + ".gz") else 
		(config.getInputFolder + x + ".gz")
	val fqFileName = config.getTmpFolder + x
	val mtFileWrite = false
	
	var t0 = System.currentTimeMillis
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
		{
			HDFSManager.create(config.getHadoopInstall, config.getOutputFolder + "log/bwa/" + x)
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
	
	if (!Files.exists(Paths.get(getRefFilePath(config))))
	{
		dbgLog("bwa/" + x, t0, "#\tReference file " + getRefFilePath(config) + " not found on this node!", config)
		return null
	}
	
	if (unzipBWAInputDirectly)
	{
		var inputFileBytes: Array[Byte] = null
		if (config.getMode != "local")
		{
			inputFileBytes = HDFSManager.readBytes(config.getHadoopInstall, inputFileName)
			dbgLog("bwa/" + x, t0, "0a\tSize of inputFileBytes = " + inputFileBytes.size, config)
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
		HDFSManager.download(config.getHadoopInstall, x + ".gz", config.getInputFolder, config.getTmpFolder, true)
		val unzipStr = "gunzip -c " + inputFileName
		dbgLog("bwa/" + x, t0, "0a\t" + unzipStr + ", Input file size = " + ((new File(inputFileName).length) / (1024 * 1024)) + " MB. ", config)
		unzipStr #> new java.io.File(fqFileName) !;
		if (config.getMode != "local")
			new File(inputFileName).delete
	}
	dbgLog("bwa/" + x, t0, "0b\tFastq file size = " + ((new File(fqFileName).length) / (1024 * 1024)) + " MB", config)
	// bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// Example: bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// run bwa mem
	val progName = getBinToolsDirPath(config) + "bwa mem "
	val command_str = progName + getRefFilePath(config) + " " + config.getExtraBWAParams + " -t " + config.getBWAThreads.toInt + " " + fqFileName
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
	
	dbgLog("bwa/" + x, t0, "2\t" + "bwa (size = " + (new File(getBinToolsDirPath(config) + "bwa").length) + 
		") mem completed for %s -> Number of key value pairs = %d".format(x, writerMap.size), config)
	new File(fqFileName).delete()
	
	val parMap = writerMap.par
	val nThreads = config.getBWAThreads.toInt
	parMap.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(nThreads))
	val arr = parMap.map{case (k, content) => (k, content.split('\n').size, new GzipCompressor(content.toString).compress)}.toArray
	val retArr = new Array[(Long, Int)](arr.size)
	
	if (!mtFileWrite)
	{
		val bfWriter = new BinaryFileWriter(config.getTmpFolder + "cmp-" + x)
		for(i <-0 until arr.size)
		{
			bfWriter.writeRecord(arr(i))
			retArr(i) = (arr(i)._1, arr(i)._2)
		}
		bfWriter.close
		dbgLog("bwa/" + x, t0, "3a\t" + "Content written to binary file writer", config)
		
		if (config.getMode() != "local")
		{
			new File(config.getTmpFolder + ".cmp-" + x + ".crc").delete
			HDFSManager.upload(config.getHadoopInstall, "cmp-" + x, config.getTmpFolder, config.getOutputFolder + "bwaOut/")
		}
		dbgLog("bwa/" + x, t0, "3b\t" + "Content uploaded to the HDFS", config)
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
						HDFSManager.upload(config.getHadoopInstall, fileNames(thread), config.getTmpFolder(), config.getOutputFolder + "bwaOut/")
					}
					dbgLog("bwa/" + x, t0, "3b\t" + "File " + fileNames(thread) + " uploaded to the HDFS", config)
				}
			}
			threadArray(thread).start
		}
			
		for(thread <- 0 until nThreads)
			threadArray(thread).join
	}
	
	retArr
}

def getSamRecords(x: String, chrPosMap: scala.collection.Map[Long, Int], config: Configuration) : Array[(Int, Array[Byte])] = 
{
	val bfr = new BinaryFileReader
	val ab = scala.collection.mutable.ArrayBuffer.empty[(Int, Array[Byte])]
	
	if ((writeToLog == true) && (config.getMode != "local"))
		HDFSManager.create(config.getHadoopInstall, config.getOutputFolder + "log/gsr/" + x)
			
	val t0 = System.currentTimeMillis
	dbgLog("gsr/" + x, t0, "1\tDownloading...", config)
	if (config.getMode != "local")
	{
		if (readLBInputDirectly)
			bfr.setSource(HDFSManager.readBytes(config.getHadoopInstall, config.getOutputFolder + "bwaOut/" + x))
		else
		{
			HDFSManager.download(config.getHadoopInstall, x, config.getOutputFolder + "bwaOut/", config.getTmpFolder, true)
			bfr.read(config.getTmpFolder + x)
		}
	}
	else
		bfr.read(config.getTmpFolder + x)
	dbgLog("gsr/" + x, t0, "2\tCompleted reading input", config)

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
	dbgLog("gsr/" + x, t0, "3\tDone. Total records = " + count, config)
	
	return ab.toArray
}

def uploadFileToOutput(filePath: String, outputPath: String, config: Configuration) =
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
			fs.copyFromLocalFile(false, true, new org.apache.hadoop.fs.Path(config.getTmpFolder + fileName), 
				new org.apache.hadoop.fs.Path(config.getOutputFolder + outputPath + "/" + fileName))
		}
	}
	catch 
	{
		case e: Exception => errLog(outputPath, 0, 
			"\tException in uploadFileToOutput: " + ExceptionUtils.getStackTrace(e) , config) 
	}
}

def getRegion(chrRegion: Integer, samRecordsZipped: Array[Array[Byte]], config: Configuration) : (Int, Array[SAMRecord]) =
{
	val writeInString = true
	var samRecordsSorted: Array[SAMRecord] = null
	
	var t0 = System.currentTimeMillis
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
			HDFSManager.create(config.getHadoopInstall(), config.getOutputFolder + "log/getRegion/" + "region_" + chrRegion)
		if (downloadNeededFiles)
			downloadDictFile(config)
	}
	
	if (writeInString)
	{
		val srecs = scala.collection.mutable.ArrayBuffer.empty[SAMRecord]
		dbgLog("getRegion/region_" + chrRegion, t0, "1a\tCreating key value pairs", config)
		var sb = new StringBuilder(readDictFile(config))
		var count = 0
		var badLines = 0
		val limit = 5000
		for (sr <- samRecordsZipped)
		{
			sb.append(new GzipDecompressor(sr).decompress)
			count += 1
			if (count == limit)
			{
				val bwaKeyValues = new BWAKeyValuesString(new StringBufferInputStream(sb.toString), chrRegion, config)
				bwaKeyValues.setSamRecs(srecs)
				badLines += bwaKeyValues.parseSam
				bwaKeyValues.close
				
				dbgLog("getRegion/region_" + chrRegion, t0, "1b\t" + count + "records processed!", config)
				
				sb = new StringBuilder(readDictFile(config))
				count = 0
			}
		}
		val bwaKeyValues = new BWAKeyValuesString(new StringBufferInputStream(sb.toString), chrRegion, config)
		bwaKeyValues.setSamRecs(srecs)
		badLines += bwaKeyValues.parseSam
		bwaKeyValues.close
		samRecordsSorted = srecs.toArray
		dbgLog("getRegion/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of records = " + 
			count + ". Number of bad lines = " + badLines, config)
	}
	else
	{
		dbgLog("getRegion/region_" + chrRegion, t0, "1a\tCreating key value pairs", config)
		val fileName = config.getTmpFolder + chrRegion + ".bin"
		val writer = new BufferedWriter(new FileWriter(fileName))
		writer.write(readDictFile(config))
		var count = 0
		for (sr <- samRecordsZipped)
		{
			writer.write(new GzipDecompressor(sr).decompress)
			count += 1
		}
		writer.close
		dbgLog("getRegion/region_" + chrRegion, t0, "1b\tNumber of records = " + count, config)
		val bwaKeyValues = new BWAKeyValues(fileName, chrRegion, config)
		val badLines = bwaKeyValues.parseSam
		samRecordsSorted = bwaKeyValues.getArray
		bwaKeyValues.close
		new File(fileName).delete
		dbgLog("getRegion/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of bad lines = " + badLines, config)
	}
	
	// Sorting
	implicit val samRecordOrdering = new Ordering[SAMRecord] {
		override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
	}
	scala.util.Sorting.quickSort(samRecordsSorted)
	//
	
	dbgLog("getRegion/region_" + chrRegion, t0, "3\t" + samRecordsSorted.size + " reads sorted!", config)
	return (chrRegion, samRecordsSorted)
}
 
def createBAMAndBEDFiles(chrRegion: Integer, samRecordsSorted: Array[SAMRecord], config: Configuration) : (String, Int) = 
{	
	val tmpFileBase = config.getTmpFolder + chrRegion
	val tmpOut1 = tmpFileBase + "-p1.bam"
	
	if ((writeToLog == true) && (config.getMode != "local"))
		HDFSManager.create(config.getHadoopInstall, config.getOutputFolder + "log/bam/" + "region_" + chrRegion.toString)
	
	var t0 = System.currentTimeMillis
	
	dbgLog("bam/region_" + chrRegion.toString(), t0, "2\tVC creating BAM file. Number of sam records = " + samRecordsSorted.size, config)
	
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
	val r = new ChromosomeRange()
	val input = new SAMRecordIterator(samRecordsSorted, header, r)
	var count = 0
	while(input.hasNext()) 
	{
		val sam = input.next()
		/////////////////////////////////////////
		sam.setAttribute(SAMTag.RG.name(), RGID)
		/////////////////////////////////////////
		writer.addAlignment(sam)
		count += 1
	}
	input.addLastChrRange()
	val reads = input.getCount()
	writer.close()
	
	dbgLog("bam/region_" + chrRegion.toString(), t0, "3\tMaking region file. There are " + count + " reads in total.", config)
	var region: String = null 
	makeRegionFile(tmpFileBase, r, config)
	dbgLog("bam/region_" + chrRegion.toString(), t0, "4\tDone making the region file.", config)
	
	if (saveAllStages)
		uploadFileToOutput(tmpOut1, "bamOutput", config)
		
	if (markDupWithLB)
	{
		downloadPicardTools(config)
		picardPreprocess(tmpFileBase, config)
		uploadFileToOutput(tmpFileBase + ".bam", "bam", config)
		uploadFileToOutput(tmpFileBase + ".bai", "bam", config)
	}
	else
		uploadFileToOutput(tmpOut1, "bam", config)
	uploadFileToOutput(config.getTmpFolder + chrRegion + ".bed", "bam", config)
	
	dbgLog("bam/region_" + chrRegion.toString(), t0, "4\tUploaded bam and bed files to the output.", config)
	
	if (config.getMode != "local")
	{ 
		if (markDupWithLB)
		{
			new File(tmpFileBase + ".bam").delete
			new File(tmpFileBase + ".bai").delete
		}
		else
			new File(tmpOut1).delete
		new File(tmpFileBase + ".bed").delete
	}
	
	return (chrRegion.toString, samRecordsSorted.size)
}

def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
{
	if(a.getReferenceIndex == b.getReferenceIndex)
		return a.getAlignmentStart - b.getAlignmentStart
	else
		return a.getReferenceIndex - b.getReferenceIndex
}

def variantCall(chrRegion: String, config: Configuration) : (String, Integer) =
{
	if (config.getMode != "local")
	{
		val inputFile = chrRegion + (if (markDupWithLB) ".bam" else "-p1.bam")
	
		if (writeToLog == true)
			HDFSManager.create(config.getHadoopInstall, config.getOutputFolder + "log/vc/" + "region_" + chrRegion)
			
		var t0 = System.currentTimeMillis
		dbgLog("vc/region_" + chrRegion, t0, "2g\tDownloading bam and bed files to the local directory...", config)
		HDFSManager.download(config.getHadoopInstall, inputFile, config.getOutputFolder + "bam/", config.getTmpFolder, true)
		if (markDupWithLB)
			HDFSManager.download(config.getHadoopInstall, chrRegion + ".bai", config.getOutputFolder + "bam/", config.getTmpFolder, true)
		HDFSManager.download(config.getHadoopInstall, chrRegion + ".bed", config.getOutputFolder + "bam/", config.getTmpFolder, true)
		dbgLog("vc/region_" + chrRegion, t0, "2h\tCompleted download of bam and bed to the local directory!", config)
	}
	
	return (chrRegion, processBAM(chrRegion, config))
}

def processBAM(chrRegion: String, config: Configuration) : Integer =
{
	val tmpFileBase = config.getTmpFolder + chrRegion
	var t0 = System.currentTimeMillis
	
	val f = new File(tmpFileBase + ".bed");
	if(f.exists() && !f.isDirectory()) 
		dbgLog("vc/region_" + chrRegion, t0, "#+\tbed file does exist in the tmp directory!!", config)
	else
		dbgLog("vc/region_" + chrRegion, t0, "#-\tbed file does not exist in the tmp directory!!", config)
	
	if ((config.getMode != "local") && downloadNeededFiles)
	{
		dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading vcf tools", config)
		downloadVCFTools(config)
	}
	else
		dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading not required. " + (config.getMode != "local") + ", " + downloadNeededFiles, config)
	
	try 
	{
		if (!markDupWithLB)
			picardPreprocess(tmpFileBase, config)
		if ((config.getMode != "local") && downloadNeededFiles)
		{
			dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading ref files for variant calling", config)
			downloadVCFRefFiles("dlvcf/region_" + chrRegion, config)
		}
		if (doIndelRealignment)
			indelRealignment(tmpFileBase, t0, chrRegion, config)
		if ((config.getMode != "local") && downloadNeededFiles)
		{
			dbgLog("vc/region_" + chrRegion, t0, "download\tDownloading the snp file for base recalibration", config)
			downloadVCFSnpFile("dlsnp/region_" + chrRegion, config)
		}
		baseQualityScoreRecalibration(tmpFileBase, t0, chrRegion, config)
		dnaVariantCalling(tmpFileBase, t0, chrRegion, config)
		
		if (config.getMode != "local")
		{
			new File(config.getTmpFolder() + "." + chrRegion + ".vcf.crc").delete()
			HDFSManager.upload(config.getHadoopInstall(), chrRegion + ".vcf", config.getTmpFolder(), config.getOutputFolder())
		}
		
		dbgLog("vc/region_" + chrRegion, t0, "vcf\tOutput written to vcf file", config)
		return 0
	} 
	catch 
	{
		case e: Exception => {
			dbgLog("vc/region_" + chrRegion, t0, "exception\tAn exception occurred: " + ExceptionUtils.getStackTrace(e), config)
			statusLog("Variant call error:", t0, "Variant calling failed for " + chrRegion, config)
			return 1 
		}
	}
}

def readWholeFile(fname: String, config: Configuration) : String =
{
	if (config.getMode != "local")
		return HDFSManager.readWholeFile(config.getHadoopInstall, fname)
	else
		return scala.io.Source.fromFile(fname).mkString
}

def writeWholeFile(fname: String, s: String, config: Configuration)
{
	if (config.getMode != "local")
		HDFSManager.writeWholeFile(config.getHadoopInstall, fname, s)
	else
		new PrintWriter(fname) {write(s); close}
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
	val sb = new StringBuilder
	
	lines.map { line =>
		val e = line.split('\t')
		try {
		val lval = e(1).toLong
		var hval = e(2).toLong
			
		if (lval == hval)
			hval += 1
			
		sb.append(e(0) + "\t" + lval + "\t" + hval + "\n")
		} 
		catch
		{
			case e: Exception => println("Error in makeCorrectBedFile of " + bedFile + ", line = " + line)
		}
	}
	
	new PrintWriter(bedFile) {write(sb.toString); close}
	new File(bedFile + ".1").delete
}

def makeRegionFile(tmpFileBase: String, r: ChromosomeRange, config: Configuration) : (Integer, String) =
{
	val bedFile = tmpFileBase + ".bed"
	
	if (config.useExome())
	{
		val toolsFolder = getBinToolsDirPath(config)
		val exomeBed = tmpFileBase + "_exome.bed"
		
		downloadBinProgram("bedtools", config)
		val bedToolsProg = new File(getBinToolsDirPath(config) + "bedtools")
		bedToolsProg.setExecutable(true)
		
		copyExomeBed(exomeBed, config)
		
		 // write a bed file with the region!
		val bed = new File(exomeBed + "_tmp.bed")
		r.writeToBedRegionFile(bed.getAbsolutePath())
		makeCorrectBedFile(toolsFolder + "bedtools intersect -a " + exomeBed + " -b " + bed + " -header", bedFile)
		
		// Delete temporary files
		new File(exomeBed).delete()
		new File(exomeBed + "_tmp.bed").delete()
		
		if (saveAllStages)
			uploadFileToOutput(bedFile, "bed", config)
		
		return (0, bedFile)
	}
	else
	{
		val bed = new File(bedFile)
		r.writeToBedRegionFile(bed.getAbsolutePath())
		
		if (saveAllStages)
			uploadFileToOutput(bedFile, "bed", config)
		
		return (0, bedFile)
	}
}

def picardPreprocess(tmpFileBase: String, config: Configuration)
{
	val toolsFolder = getToolsDirPath(config)
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val tmpOut2 = tmpFileBase + "-p2.bam"
	val MemString = config.getExecMemX()
	val chrRegion = getFileNameFromPath(tmpFileBase)
	val logFileName = (if (markDupWithLB) "bam/region_" else "vc/region_") + chrRegion
	
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
			uploadFileToOutput(bamOut, "picardOutput", config)
		
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
	//val knownIndel = getIndelFilePath(config)
	val tmpFile1 = tmpFileBase + "-2.bam"
	val preprocess = tmpFileBase + ".bam"
	val targets = tmpFileBase + ".intervals"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = ""//if (config.getUseKnownIndels().toBoolean) (" -known " + knownIndel) else ""; 
	
	// Realigner target creator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T RealignerTargetCreator -nt " + 
	config.getGATKThreads() + " -R " + getRefFilePath(config) + " -I " + preprocess + indelStr + " -o " +
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
		uploadFileToOutput(tmpFile1, "indelOutput", config)
	
	// Delete temporary files
	dbgLog("vc/region_" + chrRegion, t0, "indel3\tDeleting files " + preprocess + " and " + targets, config)
	new File(preprocess).delete
	new File(preprocess.replace(".bam", ".bai")).delete
	new File(targets).delete
}

def baseQualityScoreRecalibration(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration)
{
	val toolsFolder = getToolsDirPath(config)
	//val knownIndel = getIndelFilePath(config)
	val knownSite = getSnpFilePath(config)
	val tmpFile1 = if (doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
	val tmpFile2 = tmpFileBase + "-3.bam"
	val table = tmpFileBase + ".table"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = ""//if (config.getUseKnownIndels().toBoolean) (" -knownSites " + knownIndel) else ""; 
	
	// Base recalibrator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T BaseRecalibrator -nct " + 
		config.getGATKThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile1 + " -o " + table + regionStr + 
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
			uploadFileToOutput(tmpFile2, "baseOutput", config)
	
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
		config.getGATKThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile2 + bqsrStr + " --genotyping_mode DISCOVERY -o " + snps + 
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

	if (config.getMode() != "local")
		HDFSManager.download(config.getHadoopInstall, chrRegion + ".vcf", config.getOutputFolder, config.getTmpFolder, true)
	
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
	
	// Delete temporary files
	if (config.getMode != "local")
		new File(fileName).delete()
	new File(fileName + ".idx").delete()
	
	return a.toArray
}
	
def writeVCF(a: Array[((Integer, Integer), String)], config: Configuration) =
{
	var fileName = if (config.getMode() != "local") config.getTmpFolder() + "sparkCombined.vcf" 
		else config.getOutputFolder() + "sparkCombined.vcf";
 
	val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
	for(i <- 0 until a.size)
		writer.write(a(i)._2 + "\n")
	writer.close()

	if (config.getMode() != "local")
		HDFSManager.upload(config.getHadoopInstall(), "sparkCombined.vcf", config.getTmpFolder(), config.getOutputFolder())
}

def getInputFileNames(dir: String, config: Configuration) : Array[String] = 
{
	val mode = config.getMode()
	
	if (mode != "local")
	{
		var inputDir = dir
		
		if (config.getInterleaved() == false)
			inputDir = inputDir + "one/"
		val a: Array[String] = HDFSManager.getFileList(config.getHadoopInstall(), inputDir)

		if (a != null)
			for(i <- 0 until a.size)
				a(i) = a(i).replace(".gz", "")
		
		return a
	}
	else
	{
		var d: File = null
	
		if (config.getInterleaved() == true)
			d = new File(dir)
		else
			d = new File(dir + "one/")
		
		if (d.exists && d.isDirectory) 
		{
			val list: List[File] = d.listFiles.filter(_.isFile).toList
			val a: Array[String] = new Array[String](list.size)
			var i = 0
			
			for(i <- 0 until list.size)
				a(i) = list(i).getName().replace(".gz", "")
			
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

def getDictFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getDictPath() else 
		config.getSfFolder() + getFileNameFromPath(config.getDictPath())
}

def getSnpFilePath(config: Configuration) : String = 
{
	return if (config.getMode() == "local") config.getSnpPath() else
		config.getSfFolder() + getFileNameFromPath(config.getSnpPath())
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

def downloadBinProgram(fileName: String, config: Configuration)
{	
	if (config.getMode != "local")
		HDFSManager.downloadIfRequired(config.getHadoopInstall, fileName, config.getToolsFolder(), config.getTmpFolder)
}

def log(fname: String, key: String, t0: Long, message: String, config: Configuration) = 
{
	val ct = System.currentTimeMillis
	val at = (ct - config.getStartTime()) / 1000
	val et = (ct - t0) / 1000
	
	if (config.getMode != "local")
	{
		val IP = InetAddress.getLocalHost().toString()
		val node = IP.substring(0, IP.indexOf('/'))
		// Node, time, absolute time, key, message
		HDFSManager.append(config.getHadoopInstall(), fname, node + "\t" + getTimeStamp() + "(" + et.toString() + " secs)\t" + 
			at.toString() + "\t" + message + "\n")
	}
	else
	{
		val s = getTimeStamp() + "(" + et.toString() + " secs)\t" + at.toString() + "\t" + message
		println(s)
		val fw = new FileWriter(fname, true) 
		fw.write(s + "\n") 
		fw.close()
	}
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

def statusLog(key: String, t0: Long, message: String, config: Configuration) =
{
	log("sparkLog.txt", key, t0, key + "\t" + message, config)
}

def dbgLog(key: String, t0: Long, message: String, config: Configuration) =
{
	if (writeToLog == true)
	{
		if (config.getMode != "local")
			log(config.getOutputFolder + "log/" + key, key, t0, message, config)
		else
			log("sparkLog.txt", key, t0, key + "\t" + message, config)
	}
}

def errLog(key: String, t0: Long, message: String, config: Configuration) =
{
	log("errorLog.txt", key, t0, key + "\t" + message, config)
}

def readDictFile(config: Configuration) : String =
{
	return scala.io.Source.fromFile(getDictFilePath(config)).mkString
}

def calcRDDSize(rdd: org.apache.spark.rdd.RDD[(Integer, (Integer, Integer, Array[Byte]))]): Long = 
{
	rdd.map(x => x._2._3).map(_.length.toLong).reduce(_+_) //add the sizes together
}

def tester (chrRegion: Integer, samRecords: Array[String], config: Configuration) =
{
	statusLog("tester", 0, "region = " + chrRegion + ", " + samRecords.size, config)
} 
	
def getTimeStamp() : String =
{
	return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "]"
}

def buildRegion(index: Int, lines: Array[Array[Byte]], config: Configuration) : (String, Int) =
{
	val region = getRegion(index, lines, config)
	return createBAMAndBEDFiles(index, region._2, config)
}

def gunZipDownloadedFile(x: String, filePath: String, config: Configuration) : Long =
{
	val fileName = getFileNameFromPath(filePath)
	val fileSize = HDFSManager.getFileSize(config.getHadoopInstall, filePath)
	
	if (!HDFSManager.exists(config.getHadoopInstall, config.getOutputFolder + "log/" + x))
		HDFSManager.create(config.getHadoopInstall, config.getOutputFolder + "log/" + x)
	
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
	val fileSize = HDFSManager.getFileSize(config.getHadoopInstall, hdfsPath)
	val f = new File(config.getSfFolder + fileName)
	
	return f.exists && (f.length == fileSize)
}

def downloadDictFile(config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
}

def downloadBWAFiles(x: String, config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	
	if (!(new File(config.getSfFolder).exists))
		new File(config.getSfFolder()).mkdirs()
	
	if (!fileToDownloadAlreadyExists(config.getRefPath, config))
	{
		HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath, config)
	}
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".amb", refFolder, config.getSfFolder)
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".ann", refFolder, config.getSfFolder)
	if (!fileToDownloadAlreadyExists(config.getRefPath + ".bwt", config))
	{
		HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".bwt.gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath + ".bwt", config)
	}
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".fai", refFolder, config.getSfFolder)
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".pac", refFolder, config.getSfFolder)
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".sa", refFolder, config.getSfFolder)
}

def downloadPicardTools(config: Configuration)
{
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "CleanSam.jar", config.getToolsFolder(), config.getSfFolder())
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "MarkDuplicates.jar", config.getToolsFolder(), config.getSfFolder())
}

def downloadVCFTools(config: Configuration)
{
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "AddOrReplaceReadGroups.jar", config.getToolsFolder(), config.getSfFolder())
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "BuildBamIndex.jar", config.getToolsFolder(), config.getSfFolder())
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "CleanSam.jar", config.getToolsFolder(), config.getSfFolder())
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "GenomeAnalysisTK.jar", config.getToolsFolder(), config.getSfFolder())
	HDFSManager.downloadIfRequired(config.getHadoopInstall, "MarkDuplicates.jar", config.getToolsFolder(), config.getSfFolder())
}

def downloadVCFRefFiles(x: String, config: Configuration)
{
	val refFolder = getDirFromPath(config.getRefPath())
	val refFileName = getFileNameFromPath(config.getRefPath())
	
	if (!(new File(config.getSfFolder).exists))
		new File(config.getSfFolder()).mkdirs()
	
	if (!fileToDownloadAlreadyExists(config.getRefPath, config))
	{
		HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".gz", refFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getRefPath, config)
	}
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	HDFSManager.downloadIfRequired(config.getHadoopInstall, refFileName + ".fai", refFolder, config.getSfFolder)
}

def downloadVCFSnpFile(x: String, config: Configuration)
{
	val snpFolder = getDirFromPath(config.getSnpPath)
	val snpFileName = getFileNameFromPath(config.getSnpPath)
	if (!fileToDownloadAlreadyExists(config.getSnpPath, config))
	{
		HDFSManager.downloadIfRequired(config.getHadoopInstall, snpFileName + ".gz", snpFolder, config.getSfFolder);
		gunZipDownloadedFile(x, config.getSnpPath, config)
	}
	HDFSManager.download(config.getHadoopInstall, snpFileName + ".idx", snpFolder, config.getSfFolder, true)	
}

def main(args: Array[String]) 
{
	val part = args(0).toInt
	val config = new Configuration()
	config.initialize()
	 
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	if (config.getMode() == "local")
	{
		conf.setMaster("local[" + config.getNumExecs() + "]")
		conf.set("spark.cores.max", config.getNumExecs())
	}
	else
	{
		conf.set("spark.shuffle.blockTransferService", "nio") 
		if (compressRDDs)
			conf.set("spark.rdd.compress","true")
		conf.set("spark.core.connection.ack.wait.timeout","6000")
		conf.set("spark.akka.frameSize","256")
	}
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.network.timeout", "12000")
	conf.set("spark.driver.maxResultSize", "2g")
	conf.set("spark.executor.heartbeatInterval", "60s")
	//conf.set("spark.yarn.executor.memoryOverhead", "2048")
	//conf.set("spark.akka.frameSize", "256")
	//conf.set("spark.executor.extraJavaOptions","-XX:-UseGCOverheadLimit")
        // FYL
        System.getProperties().storeToXML(System.out,"System Properties");

	val sc = new SparkContext(conf)
	val bcConfig = sc.broadcast(config)
	var t0 = System.currentTimeMillis
	
	if (part < 3)
	{
		sc.addSparkListener(new SparkListener() 
		{
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
			{
				bw.write(getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n");
				bw.flush
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
			{
				bw.write(getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n");
				bw.flush
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
			{
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached)
					{
						bw.write(getTimeStamp() + row.name + ": memSize = " + (row.memSize / 1000000) + "MB, diskSize " + 
							row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n");
						if (config.getMode() != "local")
						{
							statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + ": memSize = " + (row.memSize / 1000000) + 
								"MB, diskSize " + row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions, config)
						}
					}
					else if (row.name.contains("rdd_"))
					{
						if (config.getMode() != "local")
							statusLog("SparkListener:", t0, getTimeStamp() + " " + row.name + " processed!", config)
						bw.write(getTimeStamp() + row.name + " processed!\n");
					}
					bw.flush
				})
			}
		});
	}
	
	if (config.getMode != "local")
	{
		HDFSManager.create(config.getHadoopInstall(), "sparkLog.txt")
		HDFSManager.create(config.getHadoopInstall(), "errorLog.txt")
	}
	else
		new java.io.File(config.getOutputFolder).mkdirs
	
	config.print() 
	
	// Hamid: Test code //////////////////////////////////////////////////////
	// Put test code here if any
	//System.exit(1)
	//////////////////////////////////////////////////////////////////////////
	
	var i = 0
	statusLog("Spark conf: ", t0, sc.getConf.getAll.mkString("\n"), config)
	//////////////////////////////////////////////////////////////////////////
	if (part == 1)
	{
		val inputFileNames = getInputFileNames(config.getInputFolder, config)  
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
		implicit val samRecordOrdering = new Ordering[SAMRecord] {
				override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
		}
		
		statusLog("chrPos: ", t0, "downloading and sorting", config)
		val chrPositionsText = sc.textFile(config.getOutputFolder + "chrPositions/part-*")
		val chrPositionsLong = chrPositionsText.map{x => val a = x.split(':'); (a(0).toLong, a(1).toInt)}.reduceByKey(_+_).sortByKey(true)
		chrPositionsLong.persist(MEMORY_AND_DISK_SER)
		chrPositionsLong.setName("rdd_chrPositionsLong")
		val elsPerRegion = chrPositionsLong.map(x => x._2).reduce(_+_) / config.getNumRegions.toInt
	
		val regionsMap = new scala.collection.mutable.HashMap[Long, Int]
		var count = 0
		var curRegion = 0
		var chrNumber = 0
		var numOfRegions = 1
		var prevChrNumber = 0
		//
		var sb = new StringBuilder
		for (e <- chrPositionsLong.collect)
		{
			val chrPos = e._1
			chrNumber = (chrPos / SF).toInt
			val nReads = e._2
			
			if ((count > elsPerRegion) || (chrNumber != prevChrNumber))
			{
				//
				//writeWholeFile(config.getOutputFolder + "dbg/" + curRegion, sb.toString, config)
				//sb = new StringBuilder
				//
				count = 0
				curRegion += 1
				numOfRegions += 1
			}
			
			prevChrNumber = chrNumber
			count += nReads
			regionsMap(chrPos) = curRegion
			//
			//sb.append(chrPos + ":" + curRegion + "\n")
		}	
		statusLog("NumOfRegions:", t0, numOfRegions.toString, config)
		//System.exit(1)
		
		val bcChrPosMap = sc.broadcast(regionsMap)
		val inputFileNames = getInputFileNames(if (config.getMode != "local") (config.getOutputFolder + "bwaOut") else config.getTmpFolder, config)
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		val chrToSamRecord1 = inputData.flatMap(x => getSamRecords(x, bcChrPosMap.value, bcConfig.value))
		chrToSamRecord1.persist(MEMORY_AND_DISK_SER)
		chrToSamRecord1.setName("rdd_chrToSamRecord1")
		
		val rdd = chrToSamRecord1.groupByKey.map(x => buildRegion(x._1, x._2.toArray, bcConfig.value))
		rdd.persist(MEMORY_AND_DISK_SER)
		rdd.setName("rdd_bam_and_bed")
		
		sb = new StringBuilder
		for (e <- rdd.sortBy(_._2).collect)
			sb.append(e._1 + " -> " + e._2 + "\n")
		makeDirIfRequired(config.getOutputFolder + "log", config)
		writeWholeFile(config.getOutputFolder + "log/part2.txt", sb.toString, config)
	}
	else if (part == 3)
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		var inputFileNames: Array[String] = null
		if (config.getMode != "local") 
			inputFileNames = getInputFileNames(config.getOutputFolder + "bam/", config).filter(x => x.split('.')(1) == "bed").map(x => x.replace(".bed", ""))
		else 
			inputFileNames = getInputFileNames(config.getTmpFolder, config).filter(x => x.contains(".bed")).map(x => x.replace(".bed", ""))
		inputFileNames.foreach(println)
		
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		val vcf = inputData.map(x => variantCall(x, bcConfig.value)).flatMap(x=> getVCF(x._1, bcConfig.value))
		writeVCF(vcf.distinct.sortByKey(true).collect, config)
	}
	else // If some error occured at the end of part 3, you can use this part to combine all the vcf files into sparkCombined.vcf
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		var inputFileNames: Array[String] = null
		if (config.getMode != "local") 
			inputFileNames = getInputFileNames(config.getOutputFolder, config).filter(x => x.contains(".vcf")).map(x => x.replace(".vcf", ""))
		else 
			inputFileNames = getInputFileNames(config.getTmpFolder, config).filter(x => x.contains(".vcf")).map(x => x.replace(".vcf", ""))
		inputFileNames.foreach(println)
		
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		val vcf = inputData.flatMap(x=> getVCF(x, bcConfig.value))
		writeVCF(vcf.distinct.sortByKey(true).collect, config)
	}
	//////////////////////////////////////////////////////////////////////////
	var et = (System.currentTimeMillis - t0) / 1000
	statusLog("Execution time:", t0, et.toString() + "\tsecs", config)
	if (part < 3)
		sc.stop()
	bw.close()
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
