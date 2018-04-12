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

import java.io._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.ByteBuffer

class BinaryFileWriter(fileName: String)
{
	val outputStream = new FileOutputStream(fileName)
	
	def longToBytes(x: Long) : Array[Byte] =  
	{
		val buffer = ByteBuffer.allocate(8)
		buffer.putLong(x)
		return buffer.array()
	}
	
	def intToBytes(x: Int) : Array[Byte] =  
	{
		val buffer = ByteBuffer.allocate(4)
		buffer.putInt(x)
		return buffer.array()
	}
	
	def writeRecord(x: (Long, Int, Array[Byte]))
	{
		outputStream.write(longToBytes(x._1))
		outputStream.write(intToBytes(x._2))
		outputStream.write(intToBytes(x._3.size))
		outputStream.write(x._3)
	}
	
	def close() 
	{
		outputStream.close()  
	}
}
