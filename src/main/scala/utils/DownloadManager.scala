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
package utils

import tudelft.utils._
import java.io._
import sys.process._

object DownloadManager
{
	private def gunZipDownloadedFile(x: String, filePath: String, config: Configuration) : Long =
	{
		val fileName = FilesManager.getFileNameFromPath(filePath)
		val hdfsManager = new HDFSManager
		val fileSize = hdfsManager.getFileSize(filePath)
		
		LogWriter.dbgLog(x, 0, "#1\tfilePath = " + filePath + ", fileSize = " + fileSize, config)
		try{("gunzip " + config.getTmpFolder + fileName + ".gz").!}
		catch{case e: Exception => LogWriter.dbgLog(x, 0, "#gunzip\nEither already unzipped or some other thread is unzipping it!", config)}
		val f = new File(config.getTmpFolder + fileName)
		@volatile var flen = f.length
		
		var iter = 0
		while(flen != fileSize)
		{
			if ((iter % 10) == 0)
				LogWriter.dbgLog(x, 0, "#2\tflen = " + flen + ", fileSize = " + fileSize, config)
			iter += 1
			Thread.sleep(1000)
			flen = f.length
		}
		LogWriter.dbgLog(x, 0, "#3\tflen = " + flen + ", fileSize = " + fileSize, config)
		
		return flen
	}

	private def fileToDownloadAlreadyExists(hdfsPath: String, config: Configuration) : Boolean =
	{
		val fileName = FilesManager.getFileNameFromPath(hdfsPath)
		val hdfsManager = new HDFSManager
		val fileSize = hdfsManager.getFileSize(hdfsPath)
		val f = new File(config.getSfFolder + fileName)
		
		return f.exists && (f.length == fileSize)
	}

	def downloadBWAFiles(x: String, config: Configuration)
	{
		val refFolder = FilesManager.getDirFromPath(config.getRefPath())
		val refFileName = FilesManager.getFileNameFromPath(config.getRefPath())
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
		val refFolder = FilesManager.getDirFromPath(config.getRefPath())
		val refFileName = FilesManager.getFileNameFromPath(config.getRefPath())
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
			val indelFolder = FilesManager.getDirFromPath(config.getIndelPath())
			val indelFileName = FilesManager.getFileNameFromPath(config.getIndelPath())
			hdfsManager.downloadIfRequired(indelFileName, indelFolder, config.getSfFolder())
			hdfsManager.download(indelFileName + ".idx", indelFolder, config.getSfFolder, true)
		}
	}

	def downloadVCFSnpFile(x: String, config: Configuration)
	{
		val snpFolder = FilesManager.getDirFromPath(config.getSnpPath)
		val snpFileName = FilesManager.getFileNameFromPath(config.getSnpPath)
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
		val refFolder = FilesManager.getDirFromPath(config.getRefPath())
		val refFileName = FilesManager.getFileNameFromPath(config.getRefPath())
		val hdfsManager = new HDFSManager
		
		hdfsManager.downloadIfRequired(refFileName.replace(".fasta", ".dict"), refFolder, config.getSfFolder)
	}

	def downloadBinProgram(fileName: String, config: Configuration)
	{	
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
			hdfsManager.downloadIfRequired(fileName, config.getToolsFolder(), config.getTmpFolder)
	}
}
