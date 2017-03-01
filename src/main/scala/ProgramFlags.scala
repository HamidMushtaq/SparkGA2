// ProgramFlags.scala
object ProgramFlags
{
	final val saveAllStages = false
	final val compressRDDs = true
	final val useTmpDirForJava = false
	final val rmDupsInBWA = false
	final val SF = 1e12.toLong
	final val chrPosGran = 5000
	// Downloading
	final val downloadNeededFiles = false
	// Compression and reading of HDFS files
	final val unzipBWAInputDirectly = false
	final val readLBInputDirectly = true
	// Optional stages
	final val doIndelRealignment = false
	final val doPrintReads = false
}
