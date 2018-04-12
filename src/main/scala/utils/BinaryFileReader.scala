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

class BinaryFileReader()
{
	private var i: Int = 0
	private var readBytes: Array[Byte] = null
	
	def bytesToLong(bytes: Array[Byte]) : Long =
	{
		val buffer = ByteBuffer.allocate(8);
		buffer.put(bytes)
		buffer.flip()	//need flip 
		return buffer.getLong()
	}
	
	def bytesToInt(bytes: Array[Byte]) : Int =
	{
		val buffer = ByteBuffer.allocate(4);
		buffer.put(bytes)
		buffer.flip()	//need flip 
		return buffer.getInt()
	}
	
	def read(fileName: String) =
	{
		val path = Paths.get(fileName)
		readBytes = Files.readAllBytes(path)
	}
	
	def setSource(byteArray: Array[Byte]) =
	{
		readBytes = byteArray
	}
	
	def readRecord() : (Long, Int, Array[Byte]) =
	{
		val x1 = readBytes.slice(i, i+8)
		i += 8
		if (x1.isEmpty)
			return null
			
		val nSamRecsBin = readBytes.slice(i, i+4)
		i += 4
		
		val tmp = readBytes.slice(i, i+4)
		i += 4
		val sizeOfx2 = bytesToInt(tmp)
		
		val x2 = readBytes.slice(i, i+sizeOfx2)
		i += sizeOfx2
		
		return (bytesToLong(x1), bytesToInt(nSamRecsBin), x2)
	}
	
	def close()
	{
		readBytes = null
	}
}
