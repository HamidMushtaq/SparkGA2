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

import tudelft.utils._
import java.text._
import java.net._
import java.io._
import java.util.Calendar

object LogWriter
{
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}
	
	private def log(fname: String, key: String, t0: Long, message: String, config: Configuration) = 
	{
		val ct = System.currentTimeMillis
		val at = (ct - config.getStartTime()) / 1000
		val hdfsManager = new HDFSManager
	
		if (config.getMode != "local")
		{
			val IP = InetAddress.getLocalHost().toString()
			val node = IP.substring(0, IP.indexOf('/'))
			// Node, time, absolute time, key, message
			hdfsManager.append(fname, node + "\t" + getTimeStamp() + "\t" + 
				at.toString() + "\t" + message + "\n")
		}
		else
		{
			val s = getTimeStamp() + "\t" + at.toString() + "\t" + message
			println(s)
			val fw = new FileWriter(fname, true) 
			fw.write(s + "\n") 
			fw.close()
		}
	}

	def statusLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log("sparkLog.txt", key, t0, key + "\t" + message, config)
	}

	def dbgLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log(config.getOutputFolder + "log/" + key, key, t0, message, config)
	}

	def errLog(key: String, t0: Long, message: String, config: Configuration) =
	{
		log("errorLog.txt", key, t0, key + "\t" + message, config)
	}
}
