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
import tudelft.utils.Configuration
import utils.GzipCompressor

class SamRegionsParser(chunkID: String, writerMap: scala.collection.mutable.HashMap[Long, StringBuilder], config: Configuration)
{
	var mReads = 0
	var badLines = 0
	val header = new StringBuilder
	final val SF = 1e12.toLong
	final val chrPosGran = 10000
	
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
			if (fields(2) == "*")
				return 1
				
			val chr = config.getChrIndex(fields(2))
			if (chr >= 25)
				return 1
			
			val chrPos = fields(3).toInt
			val chrAndPos = chr * SF + (chrPos / chrPosGran)
			if (!writerMap.contains(chrAndPos))
				writerMap.put(chrAndPos, new StringBuilder)
			writerMap(chrAndPos).append(line + "\n")
		
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
