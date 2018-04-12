package utils

// ProgramFlags.scala
object ProgramFlags
{
	final val saveAllStages = false
	final val compressRDDs = true
	final val useTmpDirForJava = false
	final val rmDupsInBWA = false
	final val SF = 1e12.toLong
	final val chrPosGran = 5000
	final val combineVCFsSeparately = false
	// Downloading
	final val downloadNeededFiles = false
	// Compression and reading of HDFS files
	final val compressionLevel = 4
	final val unzipBWAInputDirectly = false
	final val readLBInputDirectly = true
	// Optional stages
	final val doIndelRealignment = false
	final val doPrintReads = true
	// 
	final val distFileSystem = "hdfs"
	
	override def toString() = 
	{
		val sb = new StringBuilder
		sb.append("saveAllStages = " + saveAllStages + '\n')
		sb.append("compressRDDs = " + compressRDDs + '\n')
		sb.append("useTmpDirForJava = " + useTmpDirForJava + '\n')
		sb.append("rmDupsInBWA = " + rmDupsInBWA + '\n')
		sb.append("SF = " + SF + '\n')
		sb.append("chrPosGran = " + chrPosGran + '\n')
		sb.append("downloadNeededFiles = " + downloadNeededFiles + '\n')
		sb.append("compressionLevel = " + compressionLevel + '\n')
		sb.append("unzipBWAInputDirectly = " + unzipBWAInputDirectly + '\n')
		sb.append("readLBInputDirectly = " + readLBInputDirectly + '\n')
		sb.append("doIndelRealignment = " + doIndelRealignment + '\n')
		sb.append("doPrintReads = " + doPrintReads + '\n')
		sb.append("distFileSystem = " + distFileSystem + '\n')
		
		sb.toString
	}
}
