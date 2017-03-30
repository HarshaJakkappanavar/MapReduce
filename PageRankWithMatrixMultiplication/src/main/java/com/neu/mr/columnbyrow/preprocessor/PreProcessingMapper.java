/**
 * 
 */
package com.neu.mr.columnbyrow.preprocessor;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 *	Uses the parser utility provided and parses the .bz2 formatted file to a simple application(this application) friendly file (more focused on what's important). 
 */
public class PreProcessingMapper extends Mapper<LongWritable, Text, PageRankEntity, PageRankEntity> {
	
	// Keep only html pages not containing tilde (~).
	private static Pattern namePattern = Pattern.compile("^([^~]+)$");

	private static XMLReader xmlReader = null;
	
	static {
		/**
		 *  Initialize the SAX parser attributes.
		 */
		try {
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, PageRankEntity, PageRankEntity>.Context context)
			throws IOException, InterruptedException {

		
		try {
			// Parser fills this list with linked page names.
			List<Text> linkPageNames = new LinkedList<Text>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			
			/**
			 *  Read the line from the .bz2 file and split the line into page name and html content.
			 */
			String line = value.toString();
			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			html = html.replaceAll(" & ", "&amp;");
			Matcher matcher = namePattern.matcher(pageName);
			if (matcher.find()) {
				try {
					// Parse page and fill list of linked pages.
					linkPageNames.clear();
					xmlReader.parse(new InputSource(new StringReader(html.toString())));
				} catch (Exception e) {
					// Discard ill-formatted pages.
				}
				Text entityPageName = new Text(pageName);
				IntWritable entityOutlinkSize = new IntWritable(linkPageNames.size());
				PageRankEntity pageRankEntity = new PageRankEntity(entityPageName, 
						entityOutlinkSize, linkPageNames);
				
				context.write(pageRankEntity, pageRankEntity);
				for(Text outlinkPage : linkPageNames){
					PageRankEntity outlinkPageRankEntity = new PageRankEntity(outlinkPage);
					context.write(outlinkPageRankEntity, outlinkPageRankEntity);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
