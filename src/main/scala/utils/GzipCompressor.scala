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

import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader

class GzipCompressor(data: String, compLevel: Int)
{
	def compress() : Array[Byte] = 
	{
		val bos = new ByteArrayOutputStream(data.length)
		val gzip = new GZIPOutputStreamWithLevel(bos)
		
		gzip.setLevel(compLevel)
		gzip.write(data.getBytes)
		gzip.close
		
		val compressed = bos.toByteArray
		bos.close
		return compressed
	}
}
