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
package tudelft.utils;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.io.ByteArrayOutputStream;
import java.net.*;
import java.lang.*;
import java.nio.charset.Charset;

public class HDFSManager
{
	private final static boolean DISABLE_CACHE = false;
	
	public static void create(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static boolean exists(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			return fs.exists(filenamePath);
				
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return false;
        }
	}
	
	public static void append(String hadoopInstall, String fname, String s)
	{
		String newContent = readWholeFile(hadoopInstall, fname) + s;
		writeWholeFile(hadoopInstall, fname, newContent);
	}
	
	public static String readWholeFile(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{			
			FileSystem fs = FileSystem.get(config);
			StringBuilder builder=new StringBuilder();
			byte[] buffer=new byte[8192000];
			int bytesRead;
  
			FSDataInputStream in = fs.open(new Path(fname));
			while ((bytesRead = in.read(buffer)) > 0) 
				builder.append(new String(buffer, 0, bytesRead, "UTF-8"));
			in.close();
			
			return builder.toString();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return "";
        }
	}
	
	public static byte[] readBytes(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{			
			FileSystem fs = FileSystem.get(config);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer=new byte[67108864];
			int bytesRead;
  
			FSDataInputStream in = fs.open(new Path(fname));
			while ((bytesRead = in.read(buffer)) > 0) 
				baos.write(buffer, 0, bytesRead);
			in.close();
			
			return baos.toByteArray();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return new byte[0];
        }
	}
	
	public static String readPartialFile(String hadoopInstall, String fname, int bytes)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{			
			FileSystem fs = FileSystem.get(config);
			StringBuilder builder=new StringBuilder();
			ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes+1);
			
			FSDataInputStream in = fs.open(new Path(fname));
			IOUtils.copyBytes(in, baos, bytes+1, false);
			in.close();
			
			return new String(baos.toString("UTF-8"));
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return "IOException!";
        }
	}
	
	public static void writeWholeFile(String hadoopInstall, String fname, String s)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			Charset UTF8 = Charset.forName("utf-8");
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(fout, UTF8));
			writer.write(s);
			writer.flush();
			writer.close();
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static void writeBytes(String hadoopInstall, String fname, byte[] bytes)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path filenamePath = new Path(fname);  
		
			if (fs.exists(filenamePath))
				fs.delete(filenamePath, true);
				
			FSDataOutputStream fout = fs.create(filenamePath);
			fout.write(bytes);
			fout.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public static void remove(String hadoopInstall, String fname)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{			
			FileSystem fs = FileSystem.get(config);
			fs.delete(new Path(fname), true);
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public String getLS(String dir, boolean showHidden)
	{
		try
		{
			File folder = new File(dir);
			File[] listOfFiles = folder.listFiles();
			String lenStr = Integer.toString(listOfFiles.length);
			String str = "";
			
			InetAddress IP = InetAddress.getLocalHost();
			String hostName = IP.toString();
			
			for (int i = 0; i < listOfFiles.length; i++) 
			{
				if (listOfFiles[i].isFile())
				{
					if (!(listOfFiles[i].isHidden() && !showHidden))
						str = str + String.format("%s\t%s\n", getSizeString(listOfFiles[i].length()),
							listOfFiles[i].getName());
				}
				else if (listOfFiles[i].isDirectory()) 
					str = str + "DIR:\t" + listOfFiles[i].getName() + "\n";
			}
			return "\n************\n\tNumber of files in " + dir + " of " + hostName + 
				" = " + lenStr + "\n" + str + "\n************\n";
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return "Exception in HDFSManager.getLS";
		}
	}
	
	public static int download(String hadoopInstall, String fileName, String hdfsFolder, String localFolder, boolean overwrite)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			File f = new File(localFolder + fileName);
			if (f.exists() && !overwrite)
				f.delete();
			
			fs.copyToLocalFile(new Path(hdfsFolder + fileName), 
				new Path(localFolder + fileName));
				
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public static int downloadIfRequired(String hadoopInstall, String fileName, String hdfsFolder, String localFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			File f = new File(localFolder + fileName);
			if (!f.exists())
			{
				fs.copyToLocalFile(new Path(hdfsFolder + fileName), new Path(localFolder + fileName));
			}
			else
			{
				long localFileSize = f.length();
				long hdfsFileSize = fs.getFileStatus(new Path(hdfsFolder + fileName)).getLen();
				
				if (localFileSize != hdfsFileSize)
					fs.copyToLocalFile(new Path(hdfsFolder + fileName), new Path(localFolder + fileName));
			}
			
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public static long getFileSize(String hadoopInstall, String filePath)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			Path path = new Path(filePath);  
			
			if (!fs.exists(path))
				return -1;
			else
				return fs.getFileStatus(path).getLen();
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return -2;
		}
	}
	
	public static void upload(String hadoopInstall, String fileName, String localFolder, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			
			fs.copyFromLocalFile(true, true, new Path(localFolder + fileName), 
				new Path(hdfsFolder + fileName));
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public static String[] getFileList(String hadoopInstall, String hdfsFolder)
	{
		org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
		config.addResource(new Path(hadoopInstall + "etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopInstall + "etc/hadoop/hdfs-site.xml"));
		if (DISABLE_CACHE)
			config.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		try
		{
			FileSystem fs = FileSystem.get(config); 
			FileStatus[] status = fs.listStatus(new Path(hdfsFolder));
			String[] fileNames = new String[status.length];

			for (int i=0; i < status.length; i++)
				fileNames[i] = status[i].getPath().getName();

			return fileNames;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	private String getSizeString(long len)
	{
		Float r;
		String unit;
		
		if (len > 1e9)
		{
			r = len / 1e9f;
			unit = "GB";
		}
		else if (len > 1e6)
		{
			r = len / 1e6f;
			unit = "MB";
		}
		else
		{
			r = len / 1e3f;
			unit = "KB";
		}
		
		return String.format("%.1f%s", r, unit);
	}
}