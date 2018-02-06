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
	int[] chrRegionArray;
	long chrLenSum;
	// Length of each chromosome
	ArrayList<Integer> chrLenArray;
	// <Chromosome index, Array Index>
	private HashMap<Integer, Integer> chrArrayIndexMap;
	// <Chromosome's name, index>
	private HashMap<String, Integer> chrNameMap;
	// Chrosomomes to be ignored
	private HashSet<String> ignoreListSet;
	
	public DictParser(HashSet<String> ilSet)
	{
		ignoreListSet = ilSet;
	}
	
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
	
	void setChrRegions(int regions)
	{
		int numOfBins = int(chrLenSum / 1e6); 
		int N = chrLenArray.size();
		chrRegionArray = new int[N];
		int regionSize = (int)(chrLenSum / regions);
		int currRegion = 0;
		int accSize = 0;
		
		for(int i = 0; i < N; i++)
		{
			chrRegionArray[i] = currRegion;
			accSize += chrLenArray.get(i);
			if (accSize > regionSize)
			{
				accSize = 0;
				currRegion += 1;
			}
			if (currRegion >= regions)
				currRegion = regions - 1;
		}
	}
	
	public int[] getChrRegionArray()
	{
		return chrRegionArray;
	}	
	
	public HashMap getChrNameMap()
	{
		return chrNameMap;
	}
	
	public HashMap getChrArrayIndexMap()
	{
		return chrArrayIndexMap;
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
			chrLenArray = new ArrayList<Integer>();
			int chrIndex = 0;
			int arrayIndex = 0;
			
			chrLenSum = 0;
			chrNameMap = new HashMap();
			chrArrayIndexMap = new HashMap();
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
					if (!ignoreListSet.contains(seqName))
					{
						chrLenArray.add(seqLength);
						chrLenSum += seqLength;
						chrArrayIndexMap.put(chrIndex, arrayIndex++);
					}
				} 
				catch(NumberFormatException ex) 
				{
					System.out.println("Number format exception!\n");
				}
				SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
				dict.addSequence(seq);  
				line = getLine(stream);
				chrIndex++;
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