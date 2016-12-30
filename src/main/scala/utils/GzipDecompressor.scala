package utils

import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils

class GzipDecompressor(compressed: Array[Byte])
{
	def decompress() : String = 
	{
		val out = new ByteArrayOutputStream()
		IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(compressed)), out)
		return out.toString("UTF-8")
	}
}
