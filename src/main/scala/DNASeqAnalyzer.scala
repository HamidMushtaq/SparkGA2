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

final val SF = 1e12.toLong
//////////////////////////////////////////////////////////////////////////////
def bwaRun (x: String, config: Configuration) : Array[(Long, Int)] = 
{
	var inputFileName = if ((config.getMode != "local") && !unzipBWAInputDirectly) (config.getTmpFolder + x + ".gz") else 
		(config.getInputFolder + x + ".gz")
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
			inputFileBytes = hdfsManager.readBytes(inputFileName)
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
		hdfsManager.download(x + ".gz", config.getInputFolder, config.getTmpFolder, false)
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
	
	dbgLog("bwa/" + x, t0, "2\t" + "bwa (size = " + (new File(getBinToolsDirPath(config) + "bwa").length) + 
		") mem completed for %s -> Number of key value pairs = %d".format(x, writerMap.size), config)
	new File(fqFileName).delete()
	
	val parMap = writerMap.par
	val nThreads = config.getNumThreads.toInt
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
			hdfsManager.upload("cmp-" + x, config.getTmpFolder, config.getOutputFolder + "bwaOut/")
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
						hdfsManager.upload(fileNames(thread), config.getTmpFolder(), config.getOutputFolder + "bwaOut/")
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
	val hdfsManager = new HDFSManager
	
	if ((writeToLog == true) && (config.getMode != "local"))
		hdfsManager.create(config.getOutputFolder + "log/gsr/" + x)
			
	val t0 = System.currentTimeMillis
	dbgLog("gsr/" + x, t0, "1\tDownloading...", config)
	if (config.getMode != "local")
	{
		if (readLBInputDirectly)
			bfr.setSource(hdfsManager.readBytes(config.getOutputFolder + "bwaOut/" + x))
		else
		{
			hdfsManager.download(x, config.getOutputFolder + "bwaOut/", config.getTmpFolder, false)
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

def buildRegion(index: Int, lines: Array[Array[Byte]], config: Configuration) : (String, Int) =
{
	val region = getRegion(index, lines, config)
	return createBAMAndBEDFiles(index, region._2, config)
}

def getRegion(chrRegion: Integer, samRecordsZipped: Array[Array[Byte]], config: Configuration) : (Int, Array[SAMRecord]) =
{
	val writeInString = true
	var samRecordsSorted: Array[SAMRecord] = null
	val hdfsManager = new HDFSManager
	
	var t0 = System.currentTimeMillis
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
			hdfsManager.create(config.getOutputFolder + "log/getRegion/" + "region_" + chrRegion)
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
	val hdfsManager = new HDFSManager
	
	if ((writeToLog == true) && (config.getMode != "local"))
		hdfsManager.create(config.getOutputFolder + "log/bam/" + "region_" + chrRegion.toString)
	
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
	val input = new tudelft.utils.SAMRecordIterator(samRecordsSorted, header, r)
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
		uploadFileToOutput(tmpOut1, "bamOutput", false, config)
		
	uploadFileToOutput(tmpOut1, "bam", true, config)
	uploadFileToOutput(config.getTmpFolder + chrRegion + ".bed", "bed", true, config)
	
	dbgLog("bam/region_" + chrRegion.toString(), t0, "4\tUploaded bam and bed files to the output.", config)
	
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

def processBAM(chrRegion: String, config: Configuration) : Integer =
{
	val tmpFileBase = config.getTmpFolder + chrRegion
	var t0 = System.currentTimeMillis
	val hdfsManager = new HDFSManager
	
	if (config.getMode != "local")
	{
		if (writeToLog == true)
			hdfsManager.create(config.getOutputFolder + "log/" + "region_" + chrRegion)
			
		if (!(new File(config.getTmpFolder).exists))
			new File(config.getTmpFolder).mkdirs()
		
		dbgLog("region_" + chrRegion, t0, "2g\tDownloading bam and bed files to the local directory...", config)
		hdfsManager.download(chrRegion + "-p1.bam", config.getOutputFolder + "bam/", config.getTmpFolder, false)
		hdfsManager.download(chrRegion + ".bed", config.getOutputFolder + "bed/", config.getTmpFolder, false)
		dbgLog("region_" + chrRegion, t0, "2h\tCompleted download of bam and bed to the local directory!", config)
		if (downloadNeededFiles)
		{
			dbgLog("region_" + chrRegion, t0, "*\tDownloading tools", config)
			downloadVCFTools(config)
		}
	}
	
	var f = new File(tmpFileBase + "-p1.bam");
	if(f.exists() && !f.isDirectory()) 
		dbgLog("region_" + chrRegion, t0, "*+\tBAM file does exist in the tmp directory!", config)
	else
		dbgLog("region_" + chrRegion, t0, "*-\tBAM file does not exist in the tmp directory!", config)
	
	f = new File(tmpFileBase + ".bed");
	if(f.exists() && !f.isDirectory()) 
		dbgLog("region_" + chrRegion, t0, "#+\tbed file does exist in the tmp directory!!", config)
	else
		dbgLog("region_" + chrRegion, t0, "#-\tbed file does not exist in the tmp directory!!", config)
	
	dbgLog("region_" + chrRegion, t0, "3\tPicard processing started", config)
	var cmdRes = picardPreprocess(tmpFileBase, config)
	if (downloadNeededFiles)
	{
		dbgLog("region_" + chrRegion, t0, "*\tDownloading VCF ref files", config)
		downloadVCFRefFiles("region_" + chrRegion, config)
	}
	cmdRes += indelRealignment(tmpFileBase, t0, chrRegion, config)
	if (downloadNeededFiles)
	{
		dbgLog("region_" + chrRegion, t0, "*\tDownloading snp file", config)
		downloadVCFSnpFile("region_" + chrRegion, config)
	}
	cmdRes += baseQualityScoreRecalibration(tmpFileBase, t0, chrRegion, config)
	cmdRes += DnaVariantCalling(tmpFileBase, t0, chrRegion, config)
	
	if (config.getMode() != "local")
	{
		new File(config.getTmpFolder() + "." + chrRegion + ".vcf.crc").delete()
		hdfsManager.upload(chrRegion + ".vcf", config.getTmpFolder(), config.getOutputFolder())
	}
	
	dbgLog("region_" + chrRegion, t0, "9\tOutput written to vcf file", config)
	return cmdRes
}

def variantCall (chrRegion: String, config: Configuration) : (String, Integer) =
{
	return (chrRegion, processBAM(chrRegion, config))
}

def picardPreprocess(tmpFileBase: String, config: Configuration) : Integer =
{
	val toolsFolder = getToolsDirPath(config)
	val tmpOut1 = tmpFileBase + "-p1.bam"
	val tmpOut2 = tmpFileBase + "-p2.bam"
	val MemString = config.getExecMemX()
	
	var t0 = System.currentTimeMillis
	
	var cmdStr = "java " + MemString + " -jar " + toolsFolder + "CleanSam.jar INPUT=" + tmpOut1 + " OUTPUT=" + tmpOut2
	var cmdRes = cmdStr.!
	
	val bamOut = tmpFileBase + ".bam"
	val tmpMetrics = tmpFileBase + "-metrics.txt"
	
	cmdStr = "java " + MemString + " -jar " + toolsFolder + "MarkDuplicates.jar INPUT=" + tmpOut2 + " OUTPUT=" + bamOut +
		" METRICS_FILE=" + tmpMetrics + " CREATE_INDEX=true";
	cmdRes = cmdStr.!
	
	// Hamid - Save output of picardPreprocessing
	if (saveAllStages)
		uploadFileToOutput(bamOut, "picardOutput", false, config)
	
	// Delete temporary files
	new File(tmpOut1).delete()
	new File(tmpOut2).delete()
	new File(tmpMetrics).delete()
	
	return cmdRes
}

def indelRealignment(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
{
	val toolsFolder = getToolsDirPath(config)
	val knownIndel = getIndelFilePath(config)
	val tmpFile1 = tmpFileBase + "-2.bam"
	val preprocess = tmpFileBase + ".bam"
	val targets = tmpFileBase + ".intervals"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = if (config.getUseKnownIndels().toBoolean) (" -known " + knownIndel) else ""; 
	
	// Realigner target creator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T RealignerTargetCreator -nt " + 
	config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + preprocess + indelStr + " -o " +
		targets + regionStr
	dbgLog("region_" + chrRegion, t0, "4\t" + cmdStr, config)
	var cmdRes = cmdStr.!
	
	// Indel realigner
	cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T IndelRealigner -R " + 
		getRefFilePath(config) + " -I " + preprocess + " -targetIntervals " + targets + indelStr + " -o " + tmpFile1 + regionStr
	dbgLog("region_" + chrRegion, t0, "5\t" + cmdStr, config)
	cmdRes += cmdStr.!
	
	// Hamid - Save output of indelRealignment
	if (saveAllStages)
		uploadFileToOutput(tmpFile1, "indelOutput", false, config)
	
	// Delete temporary files
	new File(preprocess).delete()
	new File(preprocess.replace(".bam", ".bai")).delete()
	new File(targets).delete()
	
	return cmdRes
}

def baseQualityScoreRecalibration(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
{
	val toolsFolder = getToolsDirPath(config)
	val knownIndel = getIndelFilePath(config)
	val knownSite = getSnpFilePath(config)
	val tmpFile1 = tmpFileBase + "-2.bam"
	val tmpFile2 = tmpFileBase + "-3.bam"
	val table = tmpFileBase + ".table"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	val indelStr = if (config.getUseKnownIndels().toBoolean) (" -knownSites " + knownIndel) else ""; 
	
	// Base recalibrator
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T BaseRecalibrator -nct " + 
		config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile1 + " -o " + table + regionStr + 
		" --disable_auto_index_creation_and_locking_when_reading_rods" + indelStr + " -knownSites " + knownSite
	dbgLog("region_" + chrRegion, t0, "6\t" + cmdStr, config)
	var cmdRes = cmdStr.!

	// Print reads
	cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T PrintReads -R " + 
		getRefFilePath(config) + " -I " + tmpFile1 + " -o " + tmpFile2 + " -BQSR " + table + regionStr 
	dbgLog("region_" + chrRegion, t0, "7\t" + cmdStr, config)
	cmdRes += cmdStr.!
	
	// Hamid - Save output of baseQualityScoreRecalibration
	if (saveAllStages)
		uploadFileToOutput(tmpFile2, "baseOutput", false, config)
	
	// Delete temporary files
	new File(tmpFile1).delete()
	new File(tmpFile1.replace(".bam", ".bai")).delete()
	new File(table).delete()
	
	return cmdRes
}

def DnaVariantCalling(tmpFileBase: String, t0: Long, chrRegion: String, config: Configuration) : Integer =
{
	val toolsFolder = getToolsDirPath(config)
	val tmpFile2 = tmpFileBase + "-3.bam"
	val snps = tmpFileBase + ".vcf"
	val MemString = config.getExecMemX()
	val regionStr = " -L " + tmpFileBase + ".bed"
	
	// Haplotype caller
	var cmdStr = "java " + MemString + " " + config.getGATKopts + " -jar " + toolsFolder + "GenomeAnalysisTK.jar -T HaplotypeCaller -nct " + 
		config.getNumThreads() + " -R " + getRefFilePath(config) + " -I " + tmpFile2 + " --genotyping_mode DISCOVERY -o " + snps + 
		" -stand_call_conf " + config.getSCC() + " -stand_emit_conf " + config.getSEC() + regionStr + 
		" --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods"
	dbgLog("region_" + chrRegion, t0, "8\t" + cmdStr, config)
	var cmdRes = cmdStr.!
	
	// Delete temporary files
	new File(tmpFile2).delete()
	new File(tmpFile2.replace(".bam", ".bai")).delete()
	new File(tmpFileBase + ".bed").delete()
	
	return cmdRes
}

def getVCF(chrRegion: String, config: Configuration) : Array[((Integer, Integer), String)] =
{
	var a = scala.collection.mutable.ArrayBuffer.empty[((Integer, Integer), String)]
	var fileName = config.getTmpFolder() + chrRegion + ".vcf"
	var commentPos = 0
	val hdfsManager = new HDFSManager
	
	if (config.getMode() != "local")
		hdfsManager.download(chrRegion + ".vcf", config.getOutputFolder, config.getTmpFolder, false)
	
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
	val hdfsManager = new HDFSManager
 
	val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
	for(i <- 0 until a.size)
		writer.write(a(i)._2 + "\n")
	writer.close()

	if (config.getMode() != "local")
		hdfsManager.upload("sparkCombined.vcf", config.getTmpFolder(), config.getOutputFolder())
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
		return scala.io.Source.fromFile(fname).mkString
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

def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize(args(0), args(1))
	val part = args(1).toInt
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	
	if (config.getMode == "local")
	{
		conf.setMaster("local[" + config.getNumInstances() + "]")
		conf.set("spark.cores.max", config.getNumInstances())
	}
	else
	{
		conf.set("spark.shuffle.blockTransferService", "nio") 
		conf.set("spark.network.timeout", "12000")
		if (part == 1)
		{
			conf.set("spark.storage.memoryFraction", "0.1") // For older version of Spark
			//conf.set("spark.memory.storageFraction", "0.1") // For Spark 1.6
			//conf.set("spark.yarn.executor.memoryOverhead", "512")
		}
	}
   
	val sc = new SparkContext(conf)
	val bcConfig = sc.broadcast(config)
	val hdfsManager = new HDFSManager
	
	config.print() 
	
	if (part == 1)
	{
		if (config.getMode != "local")
		{
			hdfsManager.create("sparkLog.txt")
			hdfsManager.create("errorLog.txt")
		}
		else
		{
			val file = new File(config.getOutputFolder + "log")
			if ((writeToLog == true) && !file.exists())
				file.mkdir()
		}
	}
	
	var t0 = System.currentTimeMillis
	val numOfRegions = config.getNumRegions.toInt
	//////////////////////////////////////////////////////////////////////////
	if (part == 1)
	{
		val inputFileNames = getInputFileNames(config.getInputFolder, config).map(x => x.replace(".gz", ""))  
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
	else // (part == 3)
	{
		// For sorting
		implicit val vcfOrdering = new Ordering[(Integer, Integer)] {
			override def compare(a: (Integer, Integer), b: (Integer, Integer)) = if (a._1 == b._1) a._2 - b._2 else a._1 - b._1;
		}
		//
		
		var inputFileNames: Array[String] = null
		if (config.getMode != "local") 
			inputFileNames = getInputFileNames(config.getOutputFolder + "bed/", config).map(x => x.replace(".bed", ""))
		else 
			inputFileNames = getInputFileNames(config.getTmpFolder, config).filter(x => x.contains(".bed")).map(x => x.replace(".bed", ""))
		inputFileNames.foreach(println)
		
		val inputData = sc.parallelize(inputFileNames, inputFileNames.size)
		val vcf = inputData.map(x => variantCall(x, bcConfig.value)).flatMap(x=> getVCF(x._1, bcConfig.value))
		writeVCF(vcf.distinct.sortByKey().collect, config)
	}
	//////////////////////////////////////////////////////////////////////////
	var et = (System.currentTimeMillis - t0) / 1000
	statusLog("Execution time:", t0, et.toString() + "\tsecs", config)
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
