package utils

import java.io._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.ByteBuffer

//++= to append to arraybuffer
// Example: http://www.javapractices.com/topic/TopicAction.do?Id=245
// https://www.caveofprogramming.com/java/java-file-reading-and-writing-files-in-java.html

// Example usage
/*
	val bfw = new BinaryFileWriter
	
	bfw.writeRecord((137, new GzipCompressor("I am hamid").compress))
	bfw.writeRecord((187, new GzipCompressor("And I am checking this class!").compress))
	bfw.write("compressed.bin")
*/

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
