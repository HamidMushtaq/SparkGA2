# SparkGA2

SparkGA2 is an Apache Spark based scalable implementation of the best practice DNA analysis pipeline by Broad Institute, which is an improvement over the similar SparkGA1 (https://github.com/HamidMushtaq/SparkGA1.git) 

A paper describing its implementation was published in PLOS ONE. So, if you use SparkGA2 for your work, please cite the following paper.

> Mushtaq H, Ahmed N, Al-Ars Z (2019) SparkGA2: Production-quality memory-efficient Apache Spark based genome analysis framework. PLOS ONE 14(12): e0224784

## Tools used

The tools that we used can be found [here](https://drive.google.com/drive/u/1/folders/1cKjn7TbxX8XO3Tok5FK-SDwHYNrt67aN). Note that you need to keep the `gatk1.zip` file here, even if you use GATK4. For GATK4, you'll need to put the GATK4 jar file also in the tools folder. Moreover, you'll need to have the following in your `config xml` file.

```
<useKnownIndels>false</useKnownIndels>
<useGATK4>true</useGATK4>
```

## Running SparkGA2

* First of all copy all the reference files to a directory on each node. This folder name corresponds to the `sfFolder` in the xml config file.

* Copy all the following tools to a local folder. This local folder corresponds to `toolsFolder` in the xml config file.
	- bwa
	- bedtools
	- CleanSam.jar
	- GenomeAnalysisTK.jar (Original)
	- MarkDuplicates.jar
	- If you are using gpu accelerated haplo type caller, make a folder gatk1, and put the corresponding modified GenomeAnalysisTK.jar and lib folder inside it. Then zip that folder to create the gatk1.zip file (zip -r gatk1.zip gatk). Even if you are not using gpu, you must make such a folder and put the GenomeAnalysisTK.jar file in it. In that case, just put the original GenomeAnalysisTK.jar file in that folder.
	- If you are using more than one gpus, lets say 4, there would be 4 modified GenomeAnalysisTK.jar files with their corresponding lib files. Put them in folders, gatk0, gatk1, gatk2 and gatk3 respectively, and zip each of them. Also create a file gpus.txt, writing the number of gpus in the first line (4, for this example).
	
* If you are using gpus, or the vectorized library, you need to copy the folder of that library to each node. For example, if you are using the vectorized library, then you use the following for the gatkOpts option in the configuration file,
	```
	<gatkOpts>-XX:+AggressiveOpts -Djava.library.path=/home/genomics/4Hamid/shanshan/files/PairHMM_Vector_Power8_Ubuntu/</gatkOpts>
	```
	Which means that the `PairHMM_Vector_Power8_Ubuntu` folder should be present in the directory `/home/genomics/4Hamid/shanshan/files/PairHMM_Vector_Power8_Ubuntu/` of each node, in this case.

* The gpu accelerated version also requires a `*.cubin` file. For example, if you look at the `runPart.py` given in this folder, you would notice the following line.
	```
	"--conf spark.executorEnv.PAIRHMM_PATH=\"/home/genomics/4Hamid/shanshan/files/pairHMMKernel.cubin\" " 
	```
	It means, you must put the `pairHMMKernel.cubin` file in the `/home/genomics/4Hamid/shanshan/files` directory of each node.

* You can make the input chunks either separately, or in parallel (in a streaming fashion) with the main program. If you want to do chunking separately, then you can issue the following command first.
	`./run.py chunker_2.11-1.0.jar config/chunker/config.xml`
	Make sure in this case you have the input *.gz files mentioned in `config/chunker/config.xml`. Alternatively, you could stream directly from the web (See config/chunker/config_ftp.xml) for an example.

* Then you can run the program by using `runAll.py`. Before running the program, you must also have the reference's *.dict file (human_g1k_v37_decoy.dict in our case) in that folder. Moreover, you must have the compiled jar file as well as the lib folder containing the htsjdk-1.143.jar. 

* If you want to run the chunker in parallel, all you need to do is to put something like the following in the inputFolder tag.
	```
	<inputFolder>config/chunker/config.xml:1280:chunks</inputFolder>
	```
	Here `config/chunker/config.xml` is the config file given to the chunker utility. 1280 is the number of chunks that you except. This should be at least equal to the number of chunks that would be created. Always make a higher guess and never a lower one. Finally, chunks is the input folder. This should be the same as the outputFolder in the chunker's config file.
