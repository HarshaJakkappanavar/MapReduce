Deliverables:

1. The source code for the sequential and parallel versions of the Java programs with a single Makefile can be found inside the project folder, “WeatherDataAnalysis”. The execution steps are mentioned below.

2. The report is found under the file name, “HW1_Report.pdf”.

3. The log files (controller and syslog) for successful run on AWS can be found under the folder, “AWS Execution logs”.
4. The final result files produced on AWS can be found under the folder, “AWS Execution output”.

Execution:

1. Extract the Deliverables, and change the directory to …/Deliverables/WeatherDataAnalysis.

2. You will find a Makefile in this directory, which is helpful for executing the programs, please find the make commands to run each of these programs.

SEQUENTIAL version					: make sequential
SEQUENTIAL (expensive Fibonacci version) 	: make sequential-expensive
NO-LOCK version						: make no-lock
NO-LOCK (expensive Fibonacci version)		: make no-lock-expensive
COARSE-LOCK version					: make coarse-lock
COARSE-LOCK (expensive Fibonacci version)	: make coarse-lock-expensive
FINE-LOCK version					: make fine-lock
FINE-LOCK (expensive Fibonacci version)		: make fine-lock-expensive
NO-SHARING version					: make no-sharing
NO-SHARING (expensive Fibonacci version)	: make no-sharing-expensive
