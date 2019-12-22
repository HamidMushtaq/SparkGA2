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
