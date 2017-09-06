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
package tudelft.utils.filemanagement;

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

public class GPFSFileManager extends FileManager
{
	public GPFSFileManager()
	{
	}
	
	public void create(String fname)
	{
		try
		{
			File f = new File(fname);
			if (f.getParentFile() != null)
				f.getParentFile().mkdirs();
			f.createNewFile();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public PrintWriter open(String fname)
	{
		try
		{
			File f = new File(fname);
			if (f.getParentFile() != null)
				f.getParentFile().mkdirs();
			PrintWriter writer = new PrintWriter(f);
			return writer;	
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return null;
        }
	}
	
	public boolean exists(String fname)
	{
		return new File(fname).exists();
	}
	
	public void append(String fname, String s)
	{
		try
		{			
			FileWriter fw = new FileWriter(fname, true); // true to append the new data
			fw.write(s);								 // appends the string to the file
			fw.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public String readWholeFile(String fname)
	{
		try
		{
			return new String(Files.readAllBytes(Paths.get(fname)), "UTF-8");
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return "";
        }
	}
	
	public byte[] readBytes(String fname)
	{
		try
		{	
			return Files.readAllBytes(Paths.get(fname));
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return new byte[0];
        }
	}
	
	public String readPartialFile(String fname, int bytes)
	{
		try
		{			
			return new String(Files.readAllBytes(Paths.get(fname)), "UTF-8");
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
			return "IOException!";
        }
	}
	
	public void writeWholeFile(String fname, String s)
	{
		try
		{
			File f = new File(fname);
			if (f.getParentFile() != null)
				f.getParentFile().mkdirs();
			PrintWriter writer = new PrintWriter(f);
			writer.write(s);
			writer.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public void writeBytes(String fname, byte[] bytes)
	{
		try
		{
			File f = new File(fname);
			if (f.getParentFile() != null)
				f.getParentFile().mkdirs();
			FileOutputStream fos = new FileOutputStream(fname);
			fos.write(bytes);
			fos.close();
		}
		catch (IOException ex) 
		{
            ex.printStackTrace();
        }
	}
	
	public void remove(String fname)
	{
		new File(fname).delete();
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
	
	public int download(String fileName, String hdfsFolder, String localFolder, boolean doNotDeleteIfExisting)
	{
		try
		{	
			String sourcePath = hdfsFolder + fileName;
			String destPath = localFolder + fileName;
			File f = new File(destPath);
			if (f.exists() && !doNotDeleteIfExisting)
				f.delete();
			FileChannel src = new FileInputStream(sourcePath).getChannel();
			FileChannel dest = new FileOutputStream(destPath).getChannel();
			dest.transferFrom(src, 0, src.size());
				
			return 1;
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return 0;
		}
	}
	
	public int downloadIfRequired(String fileName, String hdfsFolder, String localFolder)
	{
		return 0;
	}
	
	public long getFileSize(String filePath)
	{
		File f = new File(filePath);
		if (!f.exists())
			return -1;
		return f.length();
	}
	
	public void upload(String fileName, String localFolder, String hdfsFolder)
	{
		try
		{	
			String destPath = hdfsFolder + fileName;
			String sourcePath = localFolder + fileName;
			File f = new File(destPath);
			if (f.exists())
				f.delete();
			FileChannel src = new FileInputStream(sourcePath).getChannel();
			FileChannel dest = new FileOutputStream(destPath).getChannel();
			dest.transferFrom(src, 0, src.size());
			new File(sourcePath).delete();
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}
	
	public void upload(boolean delSrc, String fileName, String localFolder, String hdfsFolder)
	{
		try
		{
			String destPath = hdfsFolder + fileName;
			String sourcePath = localFolder + fileName;
			File f = new File(destPath);
			if (f.exists())
				f.delete();
			FileChannel src = new FileInputStream(sourcePath).getChannel();
			FileChannel dest = new FileOutputStream(destPath).getChannel();
			dest.transferFrom(src, 0, src.size());
			if (delSrc)
				new File(sourcePath).delete();
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
	}

	public String[] getFileList(String hdfsFolder)
	{
		try
		{
			File folder = new File(hdfsFolder);
			File[] listOfFiles = folder.listFiles();
			String[] fileNames = new String[listOfFiles.length];
			
			for (int i = 0; i < listOfFiles.length; i++) 
				fileNames[i] = listOfFiles[i].getName(); 
			
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