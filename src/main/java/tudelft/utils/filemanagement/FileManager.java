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
package tudelft.utils.filemanagement;

import java.io.*;

public abstract class FileManager
{	
	public abstract void create(String fname);
	public abstract PrintWriter open(String fname);
	public abstract boolean exists(String fname);
	public abstract void append(String fname, String s);
	public abstract String readWholeFile(String fname);
	public abstract byte[] readBytes(String fname);
	public abstract String readPartialFile(String fname, int bytes);
	public abstract void writeWholeFile(String fname, String s);
	public abstract void writeBytes(String fname, byte[] bytes);
	public abstract void remove(String fname);
	public abstract int download(String fileName, String hdfsFolder, String localFolder, boolean doNotDeleteIfExisting);
	public abstract int downloadIfRequired(String fileName, String hdfsFolder, String localFolder);
	public abstract long getFileSize(String filePath);
	public abstract void upload(String fileName, String localFolder, String hdfsFolder);
	public abstract void upload(boolean delSrc, String fileName, String localFolder, String hdfsFolder);
	public abstract String[] getFileList(String hdfsFolder);
	public abstract String getLS(String dir, boolean showHidden);
	
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