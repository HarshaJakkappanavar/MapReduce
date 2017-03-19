Deliverables:

1. The source code for NoCombiner, Combiner, InMapperCombiner and SecondarySort programs with a single Makefile can be found inside the project folder, “HW2”. The execution steps are mentioned below.

2. The report is found under, "MapReduce Algorithm/HW2_Report.pdf".

3. The output and the corresponding log files (controller and syslog) for successful run on AWS can be found under the folder, “Performance Comparison”.
Please find these files in the corresponding folders of Combiner, NoCombiner, InMapperCombiner and SecondarySort for two runs.

Execution:

1. Extract the Deliverables.

2. From the Terminal change the directory to …/Deliverables/HW2

2. You will find a Makefile in this directory, which is helpful for executing the programs, please find the make commands to run each of these programs.

NoCombiner (alone, make sure you switcht to standalone) 	: make alone-no-combiner
Combiner (alone, make sure you switch to standalone)		: make alone-combiner
InMapper combiner (alone, make sure you switch to standalone)	: make alone-in-mapper-combiner
Secondary sort (alone, make sure you switch to standalone)	: make alone-secondarysort

// Clear the output directory at the cloud and run these make commands.

NoCombiner (cloud) 						: make cloud-no-combiner
Combiner (cloud)						: make cloud-combiner-run1
InMapper combiner (cloud)					: make cloud-in-mapper-combiner-run1
Secondary sort (cloud)						: make cloud-secondary-sort

