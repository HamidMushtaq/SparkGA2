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
	private String indelPath;
	private String exomePath;
	private String inputFolder;
	private String outputFolder;
	private String toolsFolder;
	private String rgString;
	private String extraBWAParams;
	private String gatkOpts;
	private String tmpFolder;
	private String sfFolder;
	private String hadoopInstall;
	private String numInstances;
	private String numThreads;
	private String ignoreList;
	private String numRegions;
	private String numRegionsForLB;
	private SAMSequenceDictionary dict;
	private String scc;
	private String sec;
	private String useKnownIndels;
	private Long startTime;
	private String execMemGB;
	private String driverMemGB;
	private String vcMemGB;
	private int[] chrLenArray;
	private int[] chrRegionArray;
	private long chrLenSum;
	private HashMap<String, Integer> chrNameMap;
	
	public void initialize(String configFile, String part)
	{	
		try
		{
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			Document document = documentBuilder.parse(file);
			
			mode = document.getElementsByTagName("mode").item(0).getTextContent();
			refPath = document.getElementsByTagName("refPath").item(0).getTextContent();
			snpPath = document.getElementsByTagName("snpPath").item(0).getTextContent();
			indelPath = document.getElementsByTagName("indelPath").item(0).getTextContent();
			exomePath = document.getElementsByTagName("exomePath").item(0).getTextContent();
			inputFolder = correctFolderName(document.getElementsByTagName("inputFolder").item(0).getTextContent());
			outputFolder = correctFolderName(document.getElementsByTagName("outputFolder").item(0).getTextContent());
			toolsFolder = correctFolderName(document.getElementsByTagName("toolsFolder").item(0).getTextContent());
			rgString = document.getElementsByTagName("rgString").item(0).getTextContent();
			extraBWAParams = document.getElementsByTagName("extraBWAParams").item(0).getTextContent();
			gatkOpts = document.getElementsByTagName("gatkOpts").item(0).getTextContent();
			tmpFolder = correctFolderName(document.getElementsByTagName("tmpFolder").item(0).getTextContent());
			sfFolder = correctFolderName(document.getElementsByTagName("sfFolder").item(0).getTextContent());
			hadoopInstall = correctFolderName(document.getElementsByTagName("hadoopInstall").item(0).getTextContent());
			ignoreList = document.getElementsByTagName("ignoreList").item(0).getTextContent();
			numRegions = document.getElementsByTagName("numRegions").item(0).getTextContent();
			if (document.getElementsByTagName("numRegionsForLB").item(0) == null)
				numRegionsForLB = "1";
			else
				numRegionsForLB = document.getElementsByTagName("numRegionsForLB").item(0).getTextContent();
			
			numInstances = document.getElementsByTagName("numInstances" + part).item(0).getTextContent();
			if (Integer.parseInt(part) == 2)
				numThreads = document.getElementsByTagName("numTasks2").item(0).getTextContent();
			else
				numThreads = document.getElementsByTagName("numThreads" + part).item(0).getTextContent();
			execMemGB = document.getElementsByTagName("execMemGB" + part).item(0).getTextContent();
			driverMemGB = document.getElementsByTagName("driverMemGB" + part).item(0).getTextContent();
			vcMemGB = document.getElementsByTagName("vcMemGB").item(0).getTextContent();
			scc	= document.getElementsByTagName("standCC").item(0).getTextContent();
			sec	= document.getElementsByTagName("standEC").item(0).getTextContent();
			useKnownIndels = document.getElementsByTagName("useKnownIndels").item(0).getTextContent();
			
			if ((!mode.equals("local")) && (!mode.equals("hadoop")))
				throw new IllegalArgumentException("Unrecognized mode type (" + mode + "). It should be either local or hadoop.");
	
			startTime = System.currentTimeMillis();
			
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
			chrLenSum = dictParser.getChrLenSum();
			dictParser.setChrRegions(Integer.parseInt(numRegionsForLB));
			chrRegionArray = dictParser.getChrRegionArray();
			chrNameMap = dictParser.getChrNameMap();
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
		
		if (r.equals(""))
			return r;
		
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
	
	public long getChrLenSum()
	{
		return chrLenSum;
	}
	
	public int getChrRegion(int chr)
	{
		return chrRegionArray[chr];
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
	
	public String getIndelPath()
	{
		return indelPath;
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
	
	public String getRGString()
	{
		return rgString;
	}
	
	public String getExtraBWAParams()
	{
		return extraBWAParams;
	}
	
	public String getRGID()
	{
		int start = rgString.indexOf("ID:");
		int end = rgString.indexOf("\\", start);
		
		return rgString.substring(start+3, end);
	}
	
	public String getGATKopts()
	{
		return gatkOpts;
	}
	
	public String getTmpFolder()
	{
		return tmpFolder;
	}
	
	public String getSfFolder()
	{
		return sfFolder;
	}
	
	public String getNumInstances()
	{
		return numInstances;
	}
	
	public String getNumThreads()
	{
		return numThreads;
	}
	
	public String getIgnoreList()
	{
		return ignoreList;
	}
	
	public String getNumRegions()
	{
		return numRegions;
	}
	
	public String getNumRegionsForLB()
	{
		return numRegionsForLB;
	}
	
	public void setNumInstances(String numInstances)
	{
		this.numInstances = numInstances;
	}
	
	public void setNumThreads(String numThreads)
	{
		this.numThreads = numThreads;
	}

	public String getSCC()
	{
		return scc.toString();
	}
	
	public String getSEC()
	{
		return sec.toString();
	}
	
	public String getUseKnownIndels()
	{
		return useKnownIndels;
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
		Integer value = Integer.parseInt(vcMemGB) * 1024;
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
		System.out.println("ignoreList:\t" + ignoreList);
		System.out.println("numInstances:\t" + numInstances);
		System.out.println("numThreads:\t" + numThreads);
		System.out.println("execMemGB:\t" + execMemGB);
		System.out.println("driverMemGB:\t" + driverMemGB);
		for (String key : chrNameMap.keySet()) {
			if (chrNameMap.get(key) < 25)
				System.out.println("\tChromosome " + key + " -> " + chrNameMap.get(key)); 
		}
		System.out.println("*************************");
	}
}