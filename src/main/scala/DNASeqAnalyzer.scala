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
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

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
import tudelft.utils.filemanagement.FileManagerFactory
import utils._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import htsjdk.samtools.util.BufferedLineReader
import htsjdk.samtools._

object DNASeqAnalyzer 
{
//////////////////////////////////////////////////////////////////////////////
def getNodeName() : String =
{
	val IP = InetAddress.getLocalHost
	IP.toString.split("/")(0)
}

def bwaRun (chunkName: String, config: Configuration) : Array[(Long, Int)] = 
{
	val inputFileIsUnzipped = !chunkName.contains(".gz")
	val x = chunkName.replace(".gz", "")
	val gz = if (inputFileIsUnzipped) "" else ".gz"
	var inputFileName = {
		if (!ProgramFlags.unzipBWAInputDirectly && !inputFileIsUnzipped) 
			config.getTmpFolder + x + gz 
		else 
			config.getInputFolder + x + gz 
	}
	val fqFileName = config.getTmpFolder + x
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	var t0 = System.currentTimeMillis
	
	val pwLog = LogWriter.openWriter("bwa/" + x, config)
	
	if (config.getMode != "local")
	{
		val file = new File(FilesManager.getBinToolsDirPath(config) + "bwa") 
		file.setExecutable(true)
		if (ProgramFlags.downloadNeededFiles)
		{
			LogWriter.dbgLog(pwLog, t0, "download\tDownloading reference files for bwa", config)
			DownloadManager.downloadBWAFiles("dlbwa/" + x, config)
		}
	}
	
	LogWriter.dbgLog(pwLog, t0, "*\tchunkName = " + chunkName + ", x = " + x + ", inputFileName = " + inputFileName, config)
	
	if (!Files.exists(Paths.get(FilesManager.getRefFilePath(config))))
	{
		LogWriter.dbgLog(pwLog, t0, "#\tReference file " + FilesManager.getRefFilePath(config) + " not found on this node!", config)
		pwLog.close
		return null
	}
	
	hdfsManager.create(config.getOutputFolder + "log/nodes/bwa/" + getNodeName() + "/" + x)
	//////////////////////////////////////////////////////////////////////////
	if (config.isStreaming)
	{
		val chunkNum = x.split('.')(0)
		while(!FilesManager.exists(config.getInputFolder + "ulStatus/" + chunkNum, config))
		{
			if (FilesManager.exists(config.getInputFolder + "ulStatus/end.txt", config))
			{
				if (!FilesManager.exists(config.getInputFolder + "ulStatus/" + chunkNum, config))
				{
					LogWriter.dbgLog(pwLog, t0, "#\tchunkNum = " + chunkNum + ", end.txt exists but this file doesn't!", config)
					pwLog.close
					return Array.empty[(Long, Int)]
				}
			}
			Thread.sleep(1000)
		}
	}
	//////////////////////////////////////////////////////////////////////////
	
	if (inputFileIsUnzipped)
	{
		val s = FilesManager.readWholeFile(inputFileName, config)
		new PrintWriter(fqFileName) {write(s); close}
		LogWriter.dbgLog(pwLog, t0, "0a\tSaved the content of unzipped file " + inputFileName + " to local file " + fqFileName, config)
	}
	else
	{
		if (ProgramFlags.unzipBWAInputDirectly)
		{
			var inputFileBytes: Array[Byte] = null
			inputFileBytes = hdfsManager.readBytes(inputFileName)
			LogWriter.dbgLog(pwLog, t0, "0a\tSize of " + inputFileName + " = " + (inputFileBytes.size / (1024 * 1024)) + " MB.", config)
			val decompressedStr = new GzipDecompressor(inputFileBytes).decompress
			new PrintWriter(fqFileName) {write(decompressedStr); close}
		}
		else
		{
			hdfsManager.download(x + ".gz", config.getInputFolder, config.getTmpFolder, false)
			val unzipStr = "gunzip -c " + inputFileName
			LogWriter.dbgLog(pwLog, t0, "0a\t" + unzipStr + ", Input file size = " + ((new File(inputFileName).length) / (1024 * 1024)) + " MB.", config)
			unzipStr #> new java.io.File(fqFileName) !;
			new File(inputFileName).delete
		}
	}
	LogWriter.dbgLog(pwLog, t0, "0b\tFastq file size = " + ((new File(fqFileName).length) / (1024 * 1024)) + " MB", config)
	// bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// Example: bwa mem input_files_directory/fasta_file.fasta -p -t 2 x.fq > out_file
	// run bwa mem
	val progName = FilesManager.getBinToolsDirPath(config) + "bwa mem "
	val command_str = progName + FilesManager.getRefFilePath(config) + " " + config.getExtraBWAParams + " -t " + config.getNumThreads + " " + fqFileName
	var writerMap = new scala.collection.mutable.HashMap[Long, StringBuilder]()
	
	LogWriter.dbgLog(pwLog, t0, "1\tbwa mem started: " + command_str, config)
	val samRegionsParser = new SamRegionsParser(x, writerMap, config)
	val errLog = hdfsManager.open(config.getOutputFolder + "stderr/bwa/" + x + ".err")
	val logger = ProcessLogger(
		(o: String) => {
			samRegionsParser.append(o)
			},
		(e: String) => {errLog.println(e)} // do nothing
	)
	command_str ! logger;
	errLog.close
	
	LogWriter.dbgLog(pwLog, t0, "2a\t" + "bwa (size = " + (new File(FilesManager.getBinToolsDirPath(config) + "bwa").length) + 
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
			val numOfLines = lines.size
			if (ProgramFlags.rmDupsInBWA && (numOfLines > 500))
			{
				val chrNum = k / ProgramFlags.SF
				val pos = k % ProgramFlags.SF
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
				sbDbg.append(s"($chrNum, $pos):\t" + numOfLines + " to " + reads + "\n")
				(k, reads, new GzipCompressor(sb.toString, ProgramFlags.compressionLevel).compress)
			}
			else
				//(k, content.split('\n').size, new GzipCompressor(content.toString).compress)
				(k, numOfLines, new GzipCompressor(content.toString, ProgramFlags.compressionLevel).compress)
		}
	}.toArray
	LogWriter.dbgLog(pwLog, t0, "2b\tCompressed the data.\n" + sbDbg.toString, config)
	
	val retArr = new Array[(Long, Int)](arr.size)
	
	val bfWriter = new Array[BinaryFileWriter](config.getNumRegionsForLB.toInt)
	for(i <- 0 until config.getNumRegionsForLB.toInt)
		bfWriter(i) = new BinaryFileWriter(config.getTmpFolder + i + "_cmp-" + x)
	if (config.getNumRegionsForLB == 1)
	{
		for(i <-0 until arr.size)
		{
			bfWriter(0).writeRecord(arr(i))
			retArr(i) = (arr(i)._1, arr(i)._2)
		}
	}
	else
	{
		for(i <-0 until arr.size)
		{
			val chrIndex = (arr(i)._1 / ProgramFlags.SF).toInt
			val chrArrayIndex = config.getChrArrayIndex(chrIndex)
			val chrPos = arr(i)._1 % ProgramFlags.SF
			var region = config.getChrRegion(chrArrayIndex, chrPos)
			
			if (region == -1)
			{
				region = config.getNumRegionsForLB - 1
				LogWriter.dbgLog(pwLog, t0, "ERROR\t" + "getChrRegion error for " + chrIndex + ", " + chrPos + ". Region assigned = " + region, config)
				println("ERROR\t" + "getChrRegion error for " + chrIndex + ", " + chrPos + ". Region assigned = " + region)
			}
			bfWriter(region).writeRecord(arr(i))
			retArr(i) = (arr(i)._1, arr(i)._2)
		}
	}
	for(i <- 0 until config.getNumRegionsForLB.toInt)
	{
		bfWriter(i).close
	
		new File(config.getTmpFolder + "." + i + "_cmp-" + x + ".crc").delete
		hdfsManager.upload(i + "_cmp-" + x, config.getTmpFolder, config.getOutputFolder + "bwaOut/" + i + "/")
	}
	LogWriter.dbgLog(pwLog, t0, "3\t" + "Content uploaded to the HDFS", config)
	pwLog.close
	
	retArr
}

def getSamRecords(x: String, lbRegion: Int, chrPosMap: scala.collection.Map[Long, Int], config: Configuration) : Array[(Int, Array[Byte])] = 
{
	val bfr = new BinaryFileReader
	val ab = scala.collection.mutable.ArrayBuffer.empty[(Int, Array[Byte])]
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	hdfsManager.create(config.getOutputFolder + "log/gsr/" + lbRegion + "/" + x)
			
	val t0 = System.currentTimeMillis
	LogWriter.dbgLog("gsr/" + lbRegion + "/" + x, t0, "1\tDownloading...", config)
	
	if (ProgramFlags.readLBInputDirectly)
		bfr.setSource(hdfsManager.readBytes(config.getOutputFolder + "bwaOut/" + lbRegion + "/" + x))
	else
	{
		hdfsManager.download(x, config.getOutputFolder + "bwaOut/" + lbRegion + "/", config.getTmpFolder, false)
		bfr.read(config.getTmpFolder + x)
	}

	LogWriter.dbgLog("gsr/" + lbRegion + "/" + x, t0, "2\tCompleted reading input", config)

	var r = bfr.readRecord
	var count = 0
	while(r != null)
	{
		ab.append((chrPosMap(r._1), r._3))
		count += 1
		r = bfr.readRecord
	}
	bfr.close
	if (!ProgramFlags.readLBInputDirectly) 
		new File(config.getTmpFolder + x).delete
	LogWriter.dbgLog("gsr/" + lbRegion + "/" + x, t0, "3\tDone. Total records = " + count, config)
	
	return ab.toArray
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
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	var t0 = System.currentTimeMillis
	
	if (hdfsManager.exists(config.getOutputFolder + "log/getRegion/" + lbRegion + "/region_" + chrRegion))
	{
		if (!hdfsManager.exists(config.getOutputFolder + "log/getRegion/fault/" + lbRegion + "/region_" + chrRegion))
			hdfsManager.create(config.getOutputFolder + "log/getRegion/fault/" + lbRegion + "/region_" + chrRegion)
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\t!!! Restarted by Spark !!!", config)
		LogWriter.errLog("getRegion:", t0, "Error encountered for " + lbRegion + "/region_" + chrRegion , config)
	}
	else
		hdfsManager.create(config.getOutputFolder + "log/getRegion/" + lbRegion + "/region_" + chrRegion)

	if (ProgramFlags.downloadNeededFiles)
		DownloadManager.downloadDictFile(config)

	var samRecordsSorted: Array[SAMRecord] = null 
	if (writeInString)
	{
		val nThreads = config.getNumThreads.toInt
		val srecs = new Array[scala.collection.mutable.ArrayBuffer[SAMRecord]](nThreads)
		val threadArray = new Array[Thread](nThreads)
		val si = new Array[Int](nThreads)
		val ei = new Array[Int](nThreads)
		val totalCount = new Array[Int](nThreads)
		val badLines = new Array[Int](nThreads)
		val elsPerThread = samRecordsZipped.size / nThreads
		
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1a\tCreating key value pairs, elsPerThreads = " + 
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
					sb.append(FilesManager.readDictFile(config))
					
					for (i <- si(thread) until ei(thread))
					{
						sb.append(new GzipDecompressor(samRecordsZipped(i)).decompress)
					
						count += 1
						totalCount(thread) += 1
						if (count == limit)
						{
							val bwaSamRecs = new BWASamRecsString(new StringBufferInputStream(sb.toString), chrRegion, config)
							bwaSamRecs.setSamRecs(srecs(thread))
							badLines(thread) += bwaSamRecs.parseSam
							bwaSamRecs.close
							
							printCounter += 1
							if ((printCounter % 1000) == 0)
							{
								srecs(thread).synchronized
								{
									LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\t" + printCounter + "." + count + 
										"recs processed!, sb.size = " + sb.size, config)
								}
							}
							expectedSize = (sb.size * 1.5).toInt
							
							sb = new StringBuilder(FilesManager.readDictFile(config))
							count = 0
						}
					}
					val bwaSamRecs = new BWASamRecsString(new StringBufferInputStream(sb.toString), chrRegion, config)
					bwaSamRecs.setSamRecs(srecs(thread))
					badLines(thread) += bwaSamRecs.parseSam
					bwaSamRecs.close
					} catch 
					{
						case e: Exception =>
						{
							LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "*\tError\n" + ExceptionUtils.getStackTrace(e), config)
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
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of records = " + 
			totalCountCombined + ". Number of bad lines = " + badLinesCombined, config)
	}
	else
	{
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1a\tCreating key value pairs", config)
		val fileName = config.getTmpFolder + lbRegion + "_" + chrRegion + ".bin"
		val writer = new BufferedWriter(new FileWriter(fileName))
		writer.write(FilesManager.readDictFile(config))
		var count = 0
		for (sr <- samRecordsZipped)
		{
			writer.write(new GzipDecompressor(sr).decompress)
			count += 1
		}
		writer.close
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "1b\tNumber of records = " + count, config)
		val bwaSamRecs = new BWASamRecs(fileName, chrRegion, config)
		val badLines = bwaSamRecs.parseSam
		samRecordsSorted = bwaSamRecs.getArray
		bwaSamRecs.close
		new File(fileName).delete
		LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "2\tSorting " + samRecordsSorted.size + " reads. Number of bad lines = " + badLines, config)
	}
	
	// Sorting
	implicit val samRecordOrdering = new Ordering[SAMRecord] {
		override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
	}
	scala.util.Sorting.quickSort(samRecordsSorted)
	//
	
	LogWriter.dbgLog("getRegion/" + lbRegion + "/region_" + chrRegion, t0, "3\t" + samRecordsSorted.size + " reads sorted!", config)
	return (chrRegion, samRecordsSorted)
}

def createBAMAndBEDFiles(chrRegion: Integer, lbRegion: Integer, samRecordsSorted: Array[SAMRecord], config: Configuration) : (String, Int) = 
{	
	val tmpFileBase = config.getTmpFolder + lbRegion + "_" + chrRegion
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	hdfsManager.create(config.getOutputFolder + "log/bam/" + lbRegion + "/region_" + chrRegion.toString)
	
	var t0 = System.currentTimeMillis
	
	LogWriter.dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "2\tVC creating BAM file. Number of sam records = " + 
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
				LogWriter.dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString, t0, "*\tError\nSam =  " + sam, config)
				LogWriter.statusLog("Create BAM error:", t0, "Encountered error in a line for region " + chrRegion, config)
				badLines += 1
			}
		}
	}
	input.addLastChrRange()
	val reads = input.getCount()
	writer.close()
	
	LogWriter.dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "3\tMaking region file. There are " + count + " reads in total.", config)
	var region: String = null 
	makeRegionFile(tmpFileBase, r, config)
	LogWriter.dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "4\tDone making the region file.", config)
	
	if (ProgramFlags.saveAllStages)
		FilesManager.uploadFileToOutput(tmpOut1, "bamOutput", false, config)
		
	FilesManager.uploadFileToOutput(tmpOut1, "bam", true, config)
	FilesManager.uploadFileToOutput(config.getTmpFolder + lbRegion + "_" + chrRegion + ".bed", "bed", true, config)
	
	LogWriter.dbgLog("bam/" + lbRegion + "/region_" + chrRegion.toString(), t0, "4\tUploaded bam and bed files to the output.", config)
	
	new File(tmpOut1).delete
	new File(tmpFileBase + ".bed").delete
	
	return (chrRegion.toString, samRecordsSorted.size)
}

def makeRegionFile(tmpFileBase: String, r: ChromosomeRange, config: Configuration)
{
	val bedFile = tmpFileBase + ".bed"
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	if (config.useExome())
	{
		val toolsFolder = FilesManager.getBinToolsDirPath(config)
		val exomeBed = tmpFileBase + "_exome.bed"
		
		FilesManager.copyExomeBed(exomeBed, config)
		
		val file = new File(toolsFolder + "bedtools") 
		file.setExecutable(true)
		
		 // write a bed file with the region!
		val bed = new File(exomeBed + "_tmp.bed")
		r.writeToBedRegionFile(bed.getAbsolutePath())
		FilesManager.makeCorrectBedFile(toolsFolder + "bedtools intersect -a " + exomeBed + " -b " + bed + " -header", bedFile)
		
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

def createHeader(config: Configuration) : SAMFileHeader =
{
	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	
	return header
}

def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
{
	if(a.getReferenceIndex == b.getReferenceIndex)
		return a.getAlignmentStart - b.getAlignmentStart
	else
		return a.getReferenceIndex - b.getReferenceIndex
}

def variantCall(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
{
	val tmpFileBase = config.getTmpFolder + chrRegion
	var t0 = System.currentTimeMillis
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	hdfsManager.create(config.getOutputFolder + "log/nodes/vc/" + getNodeName() + "/" + chrRegion)
	
	val pwLog = LogWriter.openWriter("vc/region_" + chrRegion, config)
	
	if (!(new File(config.getTmpFolder).exists))
		new File(config.getTmpFolder).mkdirs()
	
	hdfsManager.download(chrRegion + "-p1.bam", config.getOutputFolder + "bam/", config.getTmpFolder, false)
	hdfsManager.download(chrRegion + ".bed", config.getOutputFolder + "bed/", config.getTmpFolder, false)
	LogWriter.dbgLog(pwLog, t0, "1\tDownloaded bam and bed files to the local directory!", config)
	
	val f = new File(tmpFileBase + ".bed");
	if (!(f.exists() && !f.isDirectory())) 
		LogWriter.dbgLog(pwLog, t0, "#-\tbed file does not exist in the tmp directory!!", config)
	
	if (ProgramFlags.downloadNeededFiles)
	{
		LogWriter.dbgLog(pwLog, t0, "download\tDownloading vcf tools", config)
		DownloadManager.downloadVCFTools(config)
	}
	
	// Hamid
	//try 
	{
		picardPreprocess(tmpFileBase, pwLog, config)
		if (ProgramFlags.downloadNeededFiles)
		{
			LogWriter.dbgLog(pwLog, t0, "download\tDownloading ref files for variant calling", config)
			DownloadManager.downloadVCFRefFiles("dlvcf/region_" + chrRegion, config)
		}
		if (ProgramFlags.doIndelRealignment)
			indelRealignment(tmpFileBase, t0, chrRegion, pwLog, config)
		baseQualityScoreRecalibration(tmpFileBase, t0, chrRegion, pwLog, config)
		dnaVariantCalling(tmpFileBase, t0, chrRegion, pwLog, config)
		
		val retArray = getVCF(chrRegion, config)
		
		LogWriter.dbgLog(pwLog, t0, "vcf\tOutput written to vcf file", config)
		pwLog.close
		return retArray
	} 
	/*catch 
	{
		case e: Exception => {
			LogWriter.dbgLog(pwLog, t0, "exception\tAn exception occurred: " + ExceptionUtils.getStackTrace(e), config)
			pwLog.close
			LogWriter.statusLog("Variant call error:", t0, "Variant calling failed for " + chrRegion, config)
			return null
		}
	}*/
}

def picardPreprocess(tmpFileBase: String, pwLog: PrintWriter, config: Configuration)
{
	val toolsFolder = FilesManager.getToolsDirPath(config)
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val tmpOut2 = tmpFileBase + "-p2.bam"
	val MemString = config.getExecMemX()
	val chrRegion = FilesManager.getFileNameFromPath(tmpFileBase)
	val MDtmpDir = if (ProgramFlags.useTmpDirForJava) (" TMP_DIR=" + config.getTmpFolder) else ""
	
	var t0 = System.currentTimeMillis
	
	LogWriter.dbgLog(pwLog, t0, "picard\tPicard processing started", config)
	
	var cmdStr = "java " + MemString + " -jar " + toolsFolder + "CleanSam.jar INPUT=" + tmpOut1 + " OUTPUT=" + tmpOut2
	var status = execCommand(cmdStr, "cleansam", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
	val bamOut = tmpFileBase + ".bam"
	val tmpMetrics = tmpFileBase + "-metrics.txt"
	
	cmdStr = "java " + MemString + " -jar " + toolsFolder + "MarkDuplicates.jar INPUT=" + tmpOut2 + " OUTPUT=" + bamOut +
		" METRICS_FILE=" + tmpMetrics + MDtmpDir + " CREATE_INDEX=true";
	status = execCommand(cmdStr, "markdup", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
	// Hamid - Save output of picardPreprocessing
	if (ProgramFlags.saveAllStages)
		FilesManager.uploadFileToOutput(bamOut, "picardOutput", false, config)
	
	// Delete temporary files
	LogWriter.dbgLog(pwLog, t0, "picard\tDeleting files " + tmpOut1 + ", " + tmpOut2 + ", and " + tmpMetrics, config)
	new File(tmpMetrics).delete

	new File(tmpOut1).delete
	new File(tmpOut2).delete
}

def indelRealignment(tmpFileBase: String, t0: Long, chrRegion: String, pwLog: PrintWriter, config: Configuration) =
{
	val toolsFolder = FilesManager.getToolsDirPath(config)
	val tmpFile1 = tmpFileBase + "-2.bam"
	val preprocess = tmpFileBase + ".bam"
	val targets = tmpFileBase + ".intervals"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = "" 
	val javaTmp = if (ProgramFlags.useTmpDirForJava) ("java -Djava.io.tmpdir=" + config.getTmpFolder) else  "java"
	
	// Realigner target creator
	var cmdStr = javaTmp + " " + MemString + " -jar " + toolsFolder + 
		"GenomeAnalysisTK.jar -T RealignerTargetCreator -nt " + config.getNumThreads() + " -R " + FilesManager.getRefFilePath(config) + 
		" -I " + preprocess + indelStr + " -o " + targets + regionStr
	LogWriter.dbgLog(pwLog, t0, "indel1\t" + cmdStr, config)
	var status = execCommand(cmdStr, "indel1", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
	// Indel realigner
	cmdStr = javaTmp + " " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + 
		"GenomeAnalysisTK.jar -T IndelRealigner -R " + FilesManager.getRefFilePath(config) + " -I " + preprocess + " -targetIntervals " + 
		targets + indelStr + " -o " + tmpFile1 + regionStr
	LogWriter.dbgLog(pwLog, t0, "indel2\t" + cmdStr, config)
	status = execCommand(cmdStr, "indel2", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
	// Hamid - Save output of indelRealignment
	if (ProgramFlags.saveAllStages)
		FilesManager.uploadFileToOutput(tmpFile1, "indelOutput", false, config)
	
	// Delete temporary files
	LogWriter.dbgLog(pwLog, t0, "indel3\tDeleting files " + preprocess + " and " + targets, config)
	new File(preprocess).delete
	new File(preprocess.replace(".bam", ".bai")).delete
	new File(targets).delete
}
	
def baseQualityScoreRecalibration(tmpFileBase: String, t0: Long, chrRegion: String, pwLog: PrintWriter, config: Configuration)
{
	val toolsFolder = FilesManager.getToolsDirPath(config)
	val knownSite = FilesManager.getSnpFilePath(config)
	val tmpFile1 = if (ProgramFlags.doIndelRealignment) (tmpFileBase + "-2.bam") else (tmpFileBase + ".bam")
	val tmpFile2 = tmpFileBase + "-3.bam"
	val table = tmpFileBase + ".table"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = "" 
	val javaTmp = if (ProgramFlags.useTmpDirForJava) ("java -Djava.io.tmpdir=" + config.getTmpFolder) else  "java"
	
	// Base recalibrator
	var cmdStr = javaTmp + " " + MemString + " -jar " + toolsFolder + 
		"GenomeAnalysisTK.jar -T BaseRecalibrator -nct " + config.getNumThreads() + " -R " + FilesManager.getRefFilePath(config) + " -I " + 
		tmpFile1 + " -o " + table + regionStr + " --disable_auto_index_creation_and_locking_when_reading_rods" + indelStr + " -knownSites " + knownSite
	LogWriter.dbgLog(pwLog, t0, "base1\t" + cmdStr, config)
	var status = execCommand(cmdStr, "base1", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)

	if (ProgramFlags.doPrintReads)
	{
		// Print reads
		cmdStr = javaTmp + " " + MemString + " -jar " + toolsFolder + 
			"GenomeAnalysisTK.jar -T PrintReads -R " + FilesManager.getRefFilePath(config) + " -I " + tmpFile1 + " -o " + tmpFile2 + 
			" -BQSR " + table + regionStr 
		LogWriter.dbgLog(pwLog, t0, "base2\t" + cmdStr, config)
		status = execCommand(cmdStr, "base2", chrRegion, t0, config)
		LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
		// Hamid - Save output of baseQualityScoreRecalibration
		if (ProgramFlags.saveAllStages)
		{
			FilesManager.uploadFileToOutput(tmpFile2, "baseOutput", false, config)
			FilesManager.uploadFileToOutput(tmpFile2.replace(".bam", ".bai"), "baseOutput", false, config)
		}
		// Delete temporary files
		LogWriter.dbgLog(pwLog, t0, "base3(doPrintReads)\tDeleting files " + tmpFile1 + " and " + table, config)
		new File(tmpFile1).delete
		new File(tmpFile1.replace(".bam", ".bai")).delete
		new File(table).delete
	}
}

def execCommand(cmdStr: String, folderID: String, chrRegion: String, t0: Long, config: Configuration) : Int = 
{
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	val outLog = hdfsManager.open(config.getOutputFolder + "stdout/" + folderID + "/" + chrRegion + ".out") 
	outLog.println(cmdStr + "\n============================\n")

	val errLog = hdfsManager.open(config.getOutputFolder + "stderr/" + folderID + "/" + chrRegion + ".err")
	val logger = ProcessLogger(
		(o: String) => {
			outLog.println(o)
			},
		(e: String) => {
			errLog.println(e)
		} 
	)
	val status = cmdStr ! logger;
	outLog.println("\n============================\nExit value = " + status + "\n============================")
	outLog.close
	errLog.close
	
	return status
}

def dnaVariantCalling(tmpFileBase: String, t0: Long, chrRegion: String, pwLog: PrintWriter, config: Configuration)
{
	val toolsFolder = FilesManager.getToolsDirPath(config)
	val gpusInfoFileName = "gpus.txt"
	val tmpFile2 = {
		if (ProgramFlags.doPrintReads) 
			tmpFileBase + "-3.bam" 
		else if (ProgramFlags.doIndelRealignment) 
			tmpFileBase + "-2.bam" 
		else 
			tmpFileBase + ".bam"
	}
	val snps = tmpFileBase + ".vcf"
	val bqsrStr = if (ProgramFlags.doPrintReads) "" else (" -BQSR " + tmpFileBase + ".table ")
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val javaTmp = if (ProgramFlags.useTmpDirForJava) ("java -Djava.io.tmpdir=" + config.getTmpFolder) else  "java"
	val standconf = if (config.getSCC == "0") " " else (" -stand_call_conf " + config.getSCC)
	val standemit = if (config.getSEC == "0") " " else (" -stand_emit_conf " + config.getSEC)
	
	var gpuNumber = 1
	val gpusInfoFile = new File(gpusInfoFileName)
	if (gpusInfoFile.exists)
	{
		val br = new BufferedReader(new FileReader(gpusInfoFileName))
		val numOfGpus = br.readLine.trim.toInt
		br.close
		val regionNum = chrRegion.split('_')(1).toInt
		gpuNumber = regionNum % numOfGpus
		LogWriter.dbgLog(pwLog, t0, "unzip0\tnumOfGpus = " + numOfGpus + ", regionNum = " + regionNum + 
			", gpuNumber = " + gpuNumber, config)
	}
	else
		LogWriter.dbgLog(pwLog, t0, "unzip0\tgpus.txt does not exist, so using gpu 1", config)
	
	val gatkFolder = "gatk" + gpuNumber
	val gatkUnzippedFolder = gatkFolder + "_" + chrRegion
	LogWriter.dbgLog(pwLog, t0, "unzip1\t" + gatkFolder + ".zip -> " + gatkUnzippedFolder, config)
	var cmdStr = "unzip " + gatkFolder + ".zip -d " + gatkUnzippedFolder 
	cmdStr.!!
	LogWriter.dbgLog(pwLog, t0, "unzip2\t" + cmdStr, config)
	
	// Haplotype caller
	cmdStr = javaTmp + " " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + 
		gatkUnzippedFolder + "/" + gatkFolder + "/GenomeAnalysisTK.jar -T HaplotypeCaller -nct " + config.getNumThreads + 
		" -R " + FilesManager.getRefFilePath(config) + 
		" -I " + tmpFile2 + bqsrStr + " --genotyping_mode DISCOVERY -o " + snps + standconf + standemit + 
		regionStr + " --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods"
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	LogWriter.dbgLog(pwLog, t0, "haplo1\t" + cmdStr, config)
	val status = execCommand(cmdStr, "haplo", chrRegion, t0, config)
	LogWriter.dbgLog(pwLog, t0, "\tExit status = " + status, config)
	
	// Delete temporary files
	LogWriter.dbgLog(pwLog, t0, "haplo2\tDeleting files " + tmpFile2 + ", " + (tmpFileBase + ".bed") +
		(if (!ProgramFlags.doPrintReads) (" and " + tmpFileBase + ".table") else "."), config)
	new File(tmpFile2).delete()
	new File(tmpFile2.replace(".bam", ".bai")).delete
	new File(tmpFileBase + ".bed").delete
	if (!ProgramFlags.doPrintReads)
		new File(tmpFileBase + ".table").delete
}
	
def getVCF(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
{
	var a = scala.collection.mutable.ArrayBuffer.empty[((Integer, Integer), String)]
	var fileName = config.getTmpFolder + chrRegion + ".vcf"
	var commentPos = 0
	
	if (!Files.exists(Paths.get(fileName)))
	{
		// For the optional Part 4 only, where the vcf file is not already in the tmp dir		
		val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
		hdfsManager.download(chrRegion + ".vcf", config.getOutputFolder + "vcOut/", config.getTmpFolder, false)
		// Hamid: Write the processor info ///////////////////////////////
		val procInfo = FilesManager.readWholeLocalFile("/proc/cpuinfo")
		hdfsManager.create(config.getOutputFolder + "log/procInfo/" + chrRegion)
		LogWriter.dbgLog("procInfo/" + chrRegion, 0, "Information about the processor:\n" + procInfo, config)	
		//////////////////////////////////////////////////////////////////
	}
	
	for (line <- Source.fromFile(fileName).getLines()) 
	{
		val c = line(0)
		if (c != '#')
		{
			val e = line.split('\t')
			val position = e(1).toInt
			var chromosome = e(0)
			var chrNumber = config.getChrIndex(chromosome)
				
			a.append(((chrNumber, position), line))
		}
		else
		{
			a.append(((-1, commentPos), line))
			commentPos = commentPos + 1
		}
	}
		
	new File(config.getTmpFolder + "." + chrRegion + ".vcf.crc").delete
	FilesManager.uploadFileToOutput(config.getTmpFolder + chrRegion + ".vcf", "vcOut", true, config)
	
	// Delete temporary file
	if (Files.exists(Paths.get(fileName + ".idx")))
		new File(fileName + ".idx").delete
	
	return a.toArray
}

def writeRegionsMap(regionsMap: scala.collection.mutable.HashMap[Long, Int], config: Configuration)
{
	val sb = new StringBuilder
	for ((k,v) <- regionsMap)
		sb.append(k + "\t" + v + "\n")
	FilesManager.writeWholeFile(config.getOutputFolder + "regionsMap.txt", sb.toString, config)
}

def readRegionsMap(regionsMap: scala.collection.mutable.HashMap[Long, Int], config: Configuration)
{
	val content = FilesManager.readWholeFile(config.getOutputFolder + "regionsMap.txt", config)
	val lines = content.split('\n')
	for (e <- lines)
	{
		val x = e.split('\t')
		regionsMap(x(0).toLong) = x(1).toInt
	}
}

def getBamFileSize(fileID: String, config: Configuration) : Long =
{
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	return hdfsManager.getFileSize(config.getOutputFolder + "bam/" + fileID + "-p1.bam")
}

def main(args: Array[String]) 
{
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	val part = args(1).toInt
	val part2Region = if (part == 2) args(2).toInt else 0
	
	val master = conf.get("spark.master")
	if (master.contains("local"))
		println("MODE == LOCAL")
	else
	{
		println("MODE = CLUSTER")
		conf.set("spark.shuffle.blockTransferService", "nio") 
		conf.set("spark.network.timeout", "12000")
		if (part == 2)
		{
			//conf.set("spark.memory.fraction", "0.85")
			//conf.set("spark.memory.storageFraction", "0.75")
			conf.set("spark.memory.fraction", "0.7")
			conf.set("spark.memory.storageFraction", "0.7")
		}
	}
	if (ProgramFlags.compressRDDs)
			conf.set("spark.rdd.compress","true")	
	conf.set("spark.driver.maxResultSize", "4g")
   
	val sc = new SparkContext(conf)
	val config = new Configuration()
	config.initialize(args(0), sc.deployMode, args(1))
	val bcConfig = sc.broadcast(config)
	val hdfsManager = FileManagerFactory.createInstance(ProgramFlags.distFileSystem, config)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	//Logger.getLogger("org").setLevel(Level.OFF);
	//Logger.getLogger("akka").setLevel(Level.OFF);
	
	config.print() 
	
	if (!hdfsManager.exists("sparkLog.txt"))
		hdfsManager.create("sparkLog.txt")
	if (!hdfsManager.exists("errorLog.txt"))
		hdfsManager.create("errorLog.txt")

	var t0 = System.currentTimeMillis
	val numOfRegions = config.getNumRegions.toInt
	// Spark Listener
	sc.addSparkListener(new SparkListener() 
	{
		override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
		{
			LogWriter.statusLog("SparkListener:", t0, LogWriter.getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n", config)
		}

		override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
		{
			LogWriter.statusLog("SparkListener:", t0, LogWriter.getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n", config)
		}

		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
		{
			val map = stageCompleted.stageInfo.rddInfos
			map.foreach(row => {
				if (row.isCached)
				{	
					LogWriter.statusLog("SparkListener:", t0, LogWriter.getTimeStamp() + " " + row.name + ": memSize = " + (row.memSize / (1024*1024)) + 
							"MB, diskSize " + row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions, config)
				}
				else if (row.name.contains("rdd_"))
				{
					LogWriter.statusLog("SparkListener:", t0, LogWriter.getTimeStamp() + " " + row.name + " processed!", config)
				}
			})
		}
	});
	//////////////////////////////////////////////////////////////////////////
	LogWriter.statusLog("Program flags:", t0, ProgramFlags.toString, config)
	if (part == 1)
	{
		if (config.isStreaming)
		{
			val groupSize = config.getStreamGroupSize.toInt
			val indexes = (0 until groupSize).toArray
			val inputFileNames = indexes.map(x => x + ".fq.gz")
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
		else
		{
			val inputFileNames = FilesManager.getInputFileNames(config.getInputFolder, config).filter(x => x.contains(".fq"))  
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
	}
	else if (part == 2)
	{	
		val regionsMap = new scala.collection.mutable.HashMap[Long, Int]
		if (part2Region == 0)
		{
			implicit val samRecordOrdering = new Ordering[SAMRecord] {
					override def compare(a: SAMRecord, b: SAMRecord) = compareSAMRecords(a,b)
			}
			
			LogWriter.statusLog("chrPos: ", t0, "downloading and sorting", config)
			val chrPositionsText = sc.textFile(config.getOutputFolder + "chrPositions/part-*")
			val chrPositionsLong = chrPositionsText.map{x => val a = x.split(':'); (a(0).toLong, a(1).toInt)}.reduceByKey(_+_).sortByKey(true)
			chrPositionsLong.persist(MEMORY_AND_DISK_SER)
			chrPositionsLong.setName("rdd_chrPositionsLong")
			val elsPerRegion = chrPositionsLong.map(x => x._2).reduce(_+_) / config.getNumRegions.toInt
		
			var count = 0
			var curRegion = 0
			var numOfRegions = 1
			LogWriter.statusLog("chrPos: ", t0, "Making regions, elsPerRegion = " + elsPerRegion, config)
			for (e <- chrPositionsLong.collect)
			{
				val chrPos = e._1
				val nReads = e._2
				val chrNum = chrPos / ProgramFlags.SF
				val pos = chrPos % ProgramFlags.SF
				
				count += nReads
				if (nReads > elsPerRegion)
					LogWriter.statusLog("chrPos: ", t0, s"chr=$chrNum, pos=$pos, region=${curRegion+1} -> $nReads, avg=$elsPerRegion", config)
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
				LogWriter.statusLog("chrPos: ", t0, "Writing regionsMap", config)
				writeRegionsMap(regionsMap, config)
			}
			LogWriter.statusLog("NumOfRegions:", t0, numOfRegions.toString, config)
		}
		else
		{
			LogWriter.statusLog("readRegionsMap: ", t0, "reading for region " + part2Region + "...", config)
			readRegionsMap(regionsMap, config)
			LogWriter.statusLog("readRegionsMap: ", t0, "done reading for region " + part2Region + ".", config)
		}	
		val bcChrPosMap = sc.broadcast(regionsMap)
		val i = part2Region
		
		val inputFileNames = FilesManager.getInputFileNames(config.getOutputFolder + "bwaOut/" + i, config)
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		// RDD[(Int, Array[Byte])]
		val chrToSamRecord1 = inputData.flatMap(x => getSamRecords(x, i, bcChrPosMap.value, bcConfig.value))
		chrToSamRecord1.setName("rdd_chrToSamRecord1_" + i)
		chrToSamRecord1.persist(MEMORY_AND_DISK_SER)
		val chrToSamRecord2 = chrToSamRecord1.mapValues(ab => Array(ab)).reduceByKey((a1, a2) => a1 ++ a2)
		chrToSamRecord2.persist(MEMORY_AND_DISK_SER)
		chrToSamRecord2.setName("rdd_chrToSamRecord2_" + i)
		LogWriter.statusLog("chrToSamRecord2: ", t0, "Number of bam files to create = " + chrToSamRecord2.count, config)
	
		val rdd = chrToSamRecord2.foreach(x => buildRegion(x._1, i, x._2, bcConfig.value))		
	}
	else if (part == 3)
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		var inputFileNames: Array[String] = null
		inputFileNames = FilesManager.getInputFileNames(config.getOutputFolder + "bed/", config).map(x => x.replace(".bed", ""))
		
		val inputData1 = sc.parallelize(inputFileNames, inputFileNames.size)
		val inputData1BySize = inputData1.map(x => (getBamFileSize(x, bcConfig.value), x))
		val inputData = inputData1BySize.sortByKey(false).map(_._2)
		inputData.setName("rdd_inputData")
		// RDD[((Integer, Integer), String)]
		val vcf = inputData.flatMap(x => variantCall(x, bcConfig.value))
		vcf.setName("rdd_vc")
		//vcf.distinct.sortByKey().map(_._2).coalesce(1, false).saveAsTextFile(config.getOutputFolder + "combinedVCF")
		val vcfCollected = vcf.distinct.sortByKey().map(_._2 + '\n').collect
		val writer = hdfsManager.open(config.getOutputFolder + "sparkCombined.vcf")
		for(e <- vcfCollected)
			writer.write(e)
		writer.close
	}
	else // You get a combined vcf file with part3 anyway. So, this part is just for the case where for some reason, you want to combine the output vcf files again.
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		var inputFileNames: Array[String] = null
		inputFileNames = FilesManager.getInputFileNames(config.getOutputFolder + "vcOut/", config).map(x => x.replace(".vcf", ""))
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		val vcf = inputData.flatMap(x => getVCF(x, bcConfig.value))
		//vcf.distinct.sortByKey().map(_._2).coalesce(1, false).saveAsTextFile(config.getOutputFolder + "combinedVCF")
		val vcfCollected = vcf.distinct.sortByKey().map(_._2 + '\n').collect
		val writer = hdfsManager.open(config.getOutputFolder + "sparkCombined.vcf")
		for(e <- vcfCollected)
			writer.write(e)
		writer.close
	}
	//////////////////////////////////////////////////////////////////////////
	var et = (System.currentTimeMillis - t0) / 1000
	LogWriter.statusLog("Execution time:", t0, et.toString() + "\tsecs", config)
	sc.stop
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
