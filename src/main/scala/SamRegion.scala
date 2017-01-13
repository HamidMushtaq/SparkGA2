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
import java.io.File
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import htsjdk.samtools.SAMRecord
import tudelft.utils.Configuration
import scala.collection.mutable._

class SamRegion(header: String, fileName: String, config: Configuration)
{
	private var minPos = 0
	private var maxPos = 0
	private var size: Long = 0
	private var sbPos = new StringBuilder
	private var pw = new PrintWriter(config.getTmpFolder + fileName)
	
	//pw.write(header)
	
	def append(chrPos: Int, line: String) = 
	{
		size += 1
		sbPos.append(chrPos + "\n")
		pw.write(line + "\n")
		
		if (maxPos == 0)
		{
			minPos = chrPos
			maxPos = chrPos
		}
		
		if (chrPos < minPos)
			minPos = chrPos
		
		if (chrPos > maxPos)
			maxPos = chrPos
	}
	
	def getSize: Long =
	{
		return size
	}
	
	def getMinPos: Int =
	{
		return minPos
	}
	
	def getMaxPos: Int = 
	{
		return maxPos
	}
	
	def getContent: String =
	{
		pw.close
		val content = new String(Files.readAllBytes(Paths.get(config.getTmpFolder + fileName))) 
		new File(config.getTmpFolder + fileName).delete
		return content
	}
	
	def getPositionsStr: String =
	{
		val positionsStr = sbPos.toString
		sbPos = null
		return positionsStr
	}
}
