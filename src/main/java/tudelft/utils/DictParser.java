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
	//////////////////////////////////////////
	// HashMap for bins
	private HashMap<Integer, Integer> chrBinMap;
	int binPosCounter;
	//////////////////////////////////////////
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

	public int getTotalNumOfBins()
	{
		return binPosCounter;
	}
	
	public HashMap<Integer, Integer> getChrBinMap()
	{
		return chrBinMap;
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
			/////////////////////////////////////////
			binPosCounter = 0;
			/////////////////////////////////////////
			int chrIndex = 0;
			int arrayIndex = 0;
			
			chrLenSum = 0;
			chrNameMap = new HashMap();
			chrArrayIndexMap = new HashMap();
			chrBinMap = new HashMap();
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
						
						int numOfBins = seqLength / (int)1e6;
						if (numOfBins == 0)
							numOfBins = 1;
						for(int i = 0; i < numOfBins; i++)
						{
							int index = chrIndex * (int)1e6 + i;
							chrBinMap.put(index, binPosCounter++);
						}
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