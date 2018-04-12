/*
 * Copyright (C) 2017-2018 TU Delft, The Netherlands
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
 *
 * Authors: Hamid Mushtaq
 *
 */
package utils

// ProgramFlags.scala
object ProgramFlags
{
	final val saveAllStages = false
	final val compressRDDs = true
	final val useTmpDirForJava = false
	final val rmDupsInBWA = false
	final val SF = 1e12.toLong
	final val chrPosGran = 5000
	final val combineVCFsSeparately = false
	// Downloading
	final val downloadNeededFiles = false
	// Compression and reading of HDFS files
	final val compressionLevel = 4
	final val unzipBWAInputDirectly = false
	final val readLBInputDirectly = true
	// Optional stages
	final val doIndelRealignment = false
	final val doPrintReads = true
	// 
	final val distFileSystem = "hdfs"
	
	override def toString() = 
	{
		val sb = new StringBuilder
		sb.append("saveAllStages = " + saveAllStages + '\n')
		sb.append("compressRDDs = " + compressRDDs + '\n')
		sb.append("useTmpDirForJava = " + useTmpDirForJava + '\n')
		sb.append("rmDupsInBWA = " + rmDupsInBWA + '\n')
		sb.append("SF = " + SF + '\n')
		sb.append("chrPosGran = " + chrPosGran + '\n')
		sb.append("downloadNeededFiles = " + downloadNeededFiles + '\n')
		sb.append("compressionLevel = " + compressionLevel + '\n')
		sb.append("unzipBWAInputDirectly = " + unzipBWAInputDirectly + '\n')
		sb.append("readLBInputDirectly = " + readLBInputDirectly + '\n')
		sb.append("doIndelRealignment = " + doIndelRealignment + '\n')
		sb.append("doPrintReads = " + doPrintReads + '\n')
		sb.append("distFileSystem = " + distFileSystem + '\n')
		
		sb.toString
	}
}
