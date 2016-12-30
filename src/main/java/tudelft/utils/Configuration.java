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

import htsjdk.samtools.*;
import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;
import java.util.*;

public class Configuration implements Serializable
{
	private String mode;
	private String refPath;
	private String snpPath;
	private String exomePath;
	private String inputFolder;
	private String outputFolder;
	private String toolsFolder;
	private String tmpFolder;
	private String sfFolder;
	private String extraBWAParams;
	private String gatkOpts;
	private String hadoopInstall;
	private String numExecs;
	private String bwaThreads;
	private String gatkThreads;
	private String numRegions;
	private String isInterleaved;
	private int minChrLength;
	private boolean isPaired;
	private String chr;
	private boolean keepChrSplitPairs;
	private double scc;
	private double sec;
	private SAMSequenceDictionary dict;
	private Long startTime;
	private String execMemGB;
	private String gatkMemGB;
	private String driverMemGB;
	private int[] chrLenArray;
	private int[] chrRegionSizeArray;
	private HashMap<String, Integer> chrNameMap;
	
	public void initialize()
	{	
		try
		{
			File file = new File("config.xml");
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			mode = document.getElementsByTagName("mode").item(0).getTextContent();
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent();
			snpPath = document.getElementsByTagName("snpPath").item(0).getTextContent();
			exomePath = document.getElementsByTagName("exomePath").item(0).getTextContent();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			sfFolder = correctFolderName(document.getElementsByTagName("sfFolder").item(0).getTextContent());
			extraBWAParams = document.getElementsByTagName("extraBWAParams").item(0).getTextContent();
			gatkOpts = document.getElementsByTagName("gatkOpts").item(0).getTextContent();
			hadoopInstall = correctFolderName(document.getElementsByTagName("hadoopInstall").item(0).getTextContent());
			numExecs = document.getElementsByTagName("numExecs").item(0).getTextContent();
			bwaThreads = document.getElementsByTagName("bwaThreads").item(0).getTextContent();
			gatkThreads = document.getElementsByTagName("gatkThreads").item(0).getTextContent();
			numRegions = document.getElementsByTagName("numRegions").item(0).getTextContent();
			isInterleaved = document.getElementsByTagName("interleaved").item(0).getTextContent();
			execMemGB = document.getElementsByTagName("execMemGB").item(0).getTextContent();
			gatkMemGB = document.getElementsByTagName("gatkMemGB").item(0).getTextContent();
			driverMemGB = document.getElementsByTagName("driverMemGB").item(0).getTextContent();
	
			isPaired 				= true;
			// Note: The following values were copied from a Halvade's run
			minChrLength 			= 125486450;
			chr 					= null;
			keepChrSplitPairs 		= true;
			scc						= 30.0;
			sec						= 30.0;
			startTime				= System.currentTimeMillis();
				
			DictParser dictParser = new DictParser();
			if (mode.equals("local"))
			{
				String dictPath = refPath.replace(".fasta", ".dict");
				System.out.println("Parsing dictionary file " + dictPath);
				dict = dictParser.parse(dictPath);
			}
			else
			{
				System.out.println("Parsing dictionary file (cluster mode) ");
				dict = dictParser.parse(getFileNameFromPath(refPath).replace(".fasta", ".dict"));
			}
			System.out.println("\n1.Hash code of dict = " + dict.hashCode() + "\n");
			chrLenArray = dictParser.getChrLenArray();
			dictParser.setChrRegionsSizes(Integer.parseInt(numRegions));
			chrRegionSizeArray = dictParser.getChrRegionSizeArray();
			chrNameMap = dictParser.getChrNameMap();
			
			print();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
		if (r.charAt(r.length() - 1) != '/')
			return r + '/';
		else
			return r;
	}
	
	private String getFileNameFromPath(String path)
	{
		return path.substring(path.lastIndexOf('/') + 1);
	}
	
	public SAMSequenceDictionary getDict()
	{
		return dict;
	}
	
	public int getChrLen(int chr)
	{
		return chrLenArray[chr];
	}

	public String getMode()
	{
		return mode;
	}
	
	public String getRefPath()
	{
		return refPath;
	}
	
	public String getDictPath()
	{
		return refPath.replace(".fasta", ".dict");
	}
	
	public String getSnpPath()
	{
		return snpPath;
	}
	
	public String getExomePath()
	{
		return exomePath;
	}
	
	public String getInputFolder()
	{
		return inputFolder;
	}
	
	public String getOutputFolder()
	{
		return outputFolder;
	}
	
	public String getToolsFolder()
	{
		return toolsFolder;
	}
	
	public String getTmpFolder()
	{
		return tmpFolder;
	}
	
	public String getSfFolder()
	{
		return sfFolder;
	}
	
	public String getNumExecs()
	{
		return numExecs;
	}
	
	public String getBWAThreads()
	{
		return bwaThreads;
	}
	
	public String getGATKThreads()
	{
		return gatkThreads;
	}
	
	public String getNumRegions()
	{
		return numRegions;
	}
	
	public boolean getInterleaved()
	{
		return (isInterleaved.equals("yes"))? true : false;
	}
	
	public void setNumExecs(String numExecs)
	{
		this.numExecs = numExecs;
	}
		
	public int getMinChrLength()
	{
		return minChrLength;
	}
	
	public boolean getIsPaired()
	{
		return isPaired;
	}
	
	public String getChr()
	{
		return chr;
	}
	
	public boolean getKeepChrSplitPairs()
	{
		return keepChrSplitPairs;
	}
	
	public String getSCC()
	{
		Double x = scc;
		
		return x.toString();
	}
	
	public String getSEC()
	{
		Double x = sec;
		
		return x.toString();
	}
	
	public Long getStartTime()
	{
		return startTime;
	}
	
	public String getDriverMemGB()
	{
		return driverMemGB + "g";
	}
	
	public String getExecMemGB()
	{
		return execMemGB + "g";
	}
	
	public String getExecMemX()
	{
		Integer value = Integer.parseInt(gatkMemGB) * 1024;
		Integer execValue = value - 1280; // 1280 mb less
		
		return "-Xmx" + execValue.toString() + "m";
	}
	
	public String getHadoopInstall()
	{
		return hadoopInstall;
	}
	
	public boolean useExome()
	{
		return !exomePath.trim().equals("");
	}

	public int getChrIndex(String chrName)
	{
		return chrNameMap.get(chrName);
	}
	
	public String getExtraBWAParams()
	{
		return extraBWAParams;
	}
	
	public String getGATKopts()
	{
		return gatkOpts;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("Mode:\t\t" + "|" + mode + "|");
		System.out.println("Use exome = " + useExome());
		System.out.println("refPath:\t" + refPath);
		System.out.println("inputFolder:\t" + inputFolder);
		System.out.println("outputFolder:\t" + outputFolder);
		System.out.println("tmpFolder:\t" + tmpFolder);
		System.out.println("hadoopInstall:\t" + hadoopInstall);
		System.out.println("execMemGB:\t" + execMemGB);
		System.out.println("driverMemGB:\t" + driverMemGB);
		for (String key : chrNameMap.keySet()) {
			if (chrNameMap.get(key) < 25)
				System.out.println("\tChromosome " + key + " -> " + chrNameMap.get(key)); 
		}
		System.out.println("*************************");
	}
}