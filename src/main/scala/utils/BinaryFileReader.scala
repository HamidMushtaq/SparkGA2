package utils

import java.io._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.ByteBuffer

//++= to append to arraybuffer
// Example: http://www.javapractices.com/topic/TopicAction.do?Id=245

// Example usage
/*
val bfr = new BinaryFileReader

bfr.read("compressed.bin")
val r1 = bfr.readRecord
val r2 = bfr.readRecord
val r3 = bfr.readRecord

if (r3 == null)
	println("done!")
*/

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
