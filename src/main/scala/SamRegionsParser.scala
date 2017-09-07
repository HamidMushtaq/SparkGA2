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
import tudelft.utils.Configuration
import utils.GzipCompressor
import utils.ProgramFlags

class SamRegionsParser(chunkID: String, writerMap: scala.collection.mutable.HashMap[Long, StringBuilder], config: Configuration)
{
	var mReads = 0
	var badLines = 0
	val header = new StringBuilder
	
	def append(line: String) : Integer = 
	{
		if (line(0) == '@')
		{
			header.append(line + '\n')
			return 0
		}
			
		try
		{
			val fields = line.split('\t')
			val flags = fields(1).toInt
			
			// Hamid: If read is unmapped 
			if ((flags & 4) > 0)
				return 1
				
			if (fields(2) == "*")
				return 1
				
			if (config.isInIgnoreList(fields(2)))
				return 1
				
			val chr = config.getChrIndex(fields(2))
			if (chr < 0)
				return 1
			
			val chrPos = fields(3).toInt
			val chrAndPos = chr * ProgramFlags.SF + (chrPos / ProgramFlags.chrPosGran)
			if (!writerMap.contains(chrAndPos))
				writerMap.put(chrAndPos, new StringBuilder)
			writerMap(chrAndPos).append(line + "\n")
			mReads += 1
		
			return 1
		}
		catch
		{
			case e: Exception => println("badline<" + line + ">"); badLines += 1; return -1
		}
    }
		
	def getNumOfReads() : Integer =
	{
		return mReads
	}
	
	def getBadLines(): Integer = 
	{
		return badLines
	}
}
