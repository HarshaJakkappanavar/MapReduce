/**
 * 
 */
package com.neu.mr.utility;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * This is utility program that helps to read from the input data file and provide a list of lines.
 * 
 * @author harsha
 *
 */
public class LoaderRoutine {
	
	/**
	 * @param filePath
	 * @return list of lines.
	 */
	public static List<String> readFile(String filePath){
		
		File inputFile = new File(filePath);
		
		try {
//			Provide a scanner to read the file.
			Scanner scanner = new Scanner(inputFile);
//			Initialize the supposed to be returned list.
			List<String> lines = new ArrayList<String>();
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
//				populate the list with only the lines that have "TMAX" in them.
				lines.add(line);
			}
			scanner.close();
			return lines;
		} catch (FileNotFoundException e) {
			System.out.println("Exception: could not find the file.");
			e.printStackTrace();
		}
		
		return null;
	}

}
