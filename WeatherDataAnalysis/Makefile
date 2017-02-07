# Makefile for Multithread Weather Data Analysis project.

# Customize these paths for your environment.
# -----------------------------------------------------------
# Input csv file
1912.csv=/home/harsha/NortheasternUniversity/Semester3/MapReduce/data/1912.csv
# -----------------------------------------------------------

# Runs the different programs

# Collect all the java files in a single text file for compilation.
collect-java-files:
	find -name "*.java" > java_files.txt

# Compile the java files
compile: collect-java-files
	javac @java_files.txt
	
# Remove the java files' collection file.
remove-collection-file:
	rm java_files.txt

# Run Sequential Program
sequential: compile remove-collection-file
	java -cp src/ com.neu.mr.program.SequentialProgram ${1912.csv}
	
# Run Sequential Expensive Program
sequential-expensive: compile remove-collection-file
	java -cp src/ com.neu.mr.program.SequentialProgram ${1912.csv} true

# Run No-Lock Program
no-lock: compile remove-collection-file
	java -cp src/ com.neu.mr.program.NoLockProgram ${1912.csv}
	
# Run No-Lock Expensive Program
no-lock-expensive: compile remove-collection-file
	java -cp src/ com.neu.mr.program.NoLockProgram ${1912.csv} true

# Run Coarse-Lock Program
coarse-lock: compile remove-collection-file
	java -cp src/ com.neu.mr.program.CoarseLockProgram ${1912.csv}
	
# Run Coarse-Lock Expensive Program
coarse-lock-expensive: compile remove-collection-file
	java -cp src/ com.neu.mr.program.CoarseLockProgram ${1912.csv} true

# Run Fine-Lock Program
fine-lock: compile remove-collection-file
	java -cp src/ com.neu.mr.program.FineLockProgram ${1912.csv}
	
# Run Fine-Lock Expensive Program
fine-lock-expensive: compile remove-collection-file
	java -cp src/ com.neu.mr.program.FineLockProgram ${1912.csv} true

# Run No-Sharing Program
no-sharing: compile remove-collection-file
	java -cp src/ com.neu.mr.program.NoSharingProgram ${1912.csv}
	
# Run No-Sharing Expensive Program
no-sharing-expensive: compile remove-collection-file
	java -cp src/ com.neu.mr.program.NoSharingProgram ${1912.csv} true

	
