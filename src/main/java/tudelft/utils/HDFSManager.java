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
package tudelft.utils;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.*;
import java.lang.*;
import java.nio.charset.Charset;
import java.nio.channels.FileChannel;
import tudelft.utils.filemanagement.*;

public class HDFSManager
{
	private final static boolean USE_GPFS = false;
	private FileManager fileManager;
	
	public HDFSManager() throws IOException
	{
		if (USE_GPFS)
			fileManager = new GPFSFileManager();
		else
			fileManager = new HadoopFileManager();
	}
	
	public void create(String fname)
	{
		fileManager.create(fname);
	}
	
	public PrintWriter open(String fname)
	{
		return fileManager.open(fname);
	}
	
	public boolean exists(String fname)
	{
		return fileManager.exists(fname);
	}
	
	public void append(String fname, String s)
	{
		fileManager.append(fname, s);
	}
	
	public String readWholeFile(String fname)
	{
		return fileManager.readWholeFile(fname);
	}
	
	public byte[] readBytes(String fname)
	{
		return fileManager.readBytes(fname);
	}
	
	public String readPartialFile(String fname, int bytes)
	{
		return fileManager.readPartialFile(fname, bytes);
	}
	
	public void writeWholeFile(String fname, String s)
	{
		fileManager.writeWholeFile(fname, s);
	}
	
	public void writeBytes(String fname, byte[] bytes)
	{
		fileManager.writeBytes(fname, bytes);
	}
	
	public void remove(String fname)
	{
		fileManager.remove(fname);
	}
	
	public String getLS(String dir, boolean showHidden)
	{
		return fileManager.getLS(dir, showHidden);
	}
	
	public int download(String fileName, String hdfsFolder, String localFolder, boolean doNotDeleteIfExisting)
	{
		return fileManager.download(fileName, hdfsFolder, localFolder, doNotDeleteIfExisting);
	}
	
	public int downloadIfRequired(String fileName, String hdfsFolder, String localFolder)
	{
		return fileManager.downloadIfRequired(fileName, hdfsFolder, localFolder);
	}
	
	public long getFileSize(String filePath)
	{
		return fileManager.getFileSize(filePath);
	}
	
	public void upload(String fileName, String localFolder, String hdfsFolder)
	{
		fileManager.upload(fileName, localFolder, hdfsFolder);
	}
	
	public void upload(boolean delSrc, String fileName, String localFolder, String hdfsFolder)
	{
		fileManager.upload(delSrc, fileName, localFolder, hdfsFolder);
	}

	public String[] getFileList(String hdfsFolder)
	{
		return fileManager.getFileList(hdfsFolder);
	}
}