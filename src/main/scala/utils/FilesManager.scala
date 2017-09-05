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
import java.nio.file.{Paths, Files}
import sys.process._
import org.apache.commons.lang3.exception.ExceptionUtils

object FilesManager
{
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
		return if (config.getMode() == "local") config.getToolsFolder() else "./"
	}

	def getBinToolsDirPath(config: Configuration) : String = 
	{
		return if (config.getMode() == "local") config.getToolsFolder() else "./"
	}

	def readWholeFile(fname: String, config: Configuration) : String =
	{
		val hdfsManager = new HDFSManager
		
		if (config.getMode != "local")
			return hdfsManager.readWholeFile(fname)
		else
			return new String(Files.readAllBytes(Paths.get(fname)))
	}
	
	def exists(filePath: String, config: Configuration) : Boolean =
	{
		if (config.getMode == "local")
			return new File(filePath).exists
		else
		{
			val hdfsManager = new HDFSManager
			return hdfsManager.exists(filePath)
		}
	}
	
	def readWholeLocalFile(fname: String) : String =
	{
		return new String(Files.readAllBytes(Paths.get(fname)))
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
	
	def uploadFileToOutput(filePath: String, outputPath: String, delSrc: Boolean, config: Configuration)
	{
		if (config.getMode != "local")
		{
			val fileName = getFileNameFromPath(filePath)
			val hdfsManager = new HDFSManager
			hdfsManager.upload(delSrc, fileName, config.getTmpFolder, config.getOutputFolder + outputPath + "/")
		}
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
			if (e.size >= 2)
			{
				val lval = e(1).toLong
				var hval = e(2).toLong
			
				if (lval == hval)
					hval += 1
			
				s += e(0) + "\t" + lval + "\t" + hval + "\n"
			}
		}
		
		new PrintWriter(bedFile) {write(s); close}
		new File(bedFile + ".1").delete()
	}
	
	def makeDirIfRequired(dir: String, config: Configuration)
	{
		if (config.getMode == "local")
		{
			val file = new File(dir)
			if (!file.exists)
				file.mkdir()
		}			
	}
}
