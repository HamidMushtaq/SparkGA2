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

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import htsjdk.samtools.util.BufferedLineReader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import java.util.*;

public class DictParser
{	
	SAMSequenceDictionary dict;
	int[] chrLenArray;
	int[] chrRegionSizeArray;
	long chrLenSum;
	private HashMap<String, Integer> chrNameMap;
	
	private String getLine(FileInputStream stream) throws IOException 
	{
		String tmp = "";
		int nlines = 0;
		try 
		{
			int c = stream.read();
			while(((char)c != '\n') && (c != -1)) 
			{
				tmp = tmp + (char)c;
				c = stream.read();
			}
			//System.err.println("|" + tmp + "|");
			return (c == -1)? null : tmp;
		} 
		catch (Exception ex) 
		{
			System.err.println("End of dict\n");
			return null;
		}
	}
	
	public long getChrLenSun()
	{
		return chrLenSum;
	}
	
	public int[] getChrLenArray()
	{
		return chrLenArray;
	}
	
	void setChrRegionsSizes(int regions)
	{
		chrRegionSizeArray = new int[25];
		int avgRegionSize = (int)(chrLenSum / regions);
		
		for(int i = 0; i < 25; i++)
		{
			int segments = chrLenArray[i] / avgRegionSize;
		
			if (segments == 0)
				segments = 1;
		
			chrRegionSizeArray[i] = chrLenArray[i] / segments;
			System.out.println("Hamid: chr" + i + " avgRegionSize = " + avgRegionSize + ", chrLen = " + chrLenArray[i]);
			System.out.println("Hamid: chr" + i + " -> segments = " + segments + ", region size = " + chrRegionSizeArray[i]);
		}
	}
	
	public int[] getChrRegionSizeArray()
	{
		return chrRegionSizeArray;
	}
	
	public HashMap getChrNameMap()
	{
		return chrNameMap;
	}
		
	public SAMSequenceDictionary parse(String dictPath) 
	{
		try 
		{
			// Note: Change the file system according to the platform you are running on.
			FileInputStream stream = new FileInputStream(new File(dictPath));
			String line = getLine(stream); // header
			dict = new SAMSequenceDictionary();
			line = getLine(stream);
			chrLenArray = new int[25];
			int chrIndex = 0;
			
			chrLenSum = 0;
			chrNameMap = new HashMap();
			while(line != null) 
			{
				// @SQ	SN:chrM	LN:16571
				String[] lineData = line.split("\\s+");
				String seqName = lineData[1].substring(lineData[1].indexOf(':') + 1);
				chrNameMap.put(seqName, chrIndex);
				int seqLength = 0;
				try 
				{
					seqLength = Integer.parseInt(lineData[2].substring(lineData[2].indexOf(':') + 1));
					if (chrIndex <= 24)
					{
						chrLenSum += seqLength;
						chrLenArray[chrIndex++] = seqLength;
					}
				} 
				catch(NumberFormatException ex) 
				{
					System.out.println("Number format exception!\n");
				}
				SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
	//                Logger.DEBUG("name: " + seq.getSequenceName() + " length: " + seq.getSequenceLength());
				dict.addSequence(seq);  
				line = getLine(stream);
			}
			stream.close();
			return dict;
		} 
		catch (IOException ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
}