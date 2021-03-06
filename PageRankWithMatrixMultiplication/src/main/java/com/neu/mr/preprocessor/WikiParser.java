/**
 * 
 */
package com.neu.mr.preprocessor;

import java.net.URLDecoder;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author harsha
 * 
 * 	Using the Parser file given as part of the HW material.
 *
 */
/** Parses a Wikipage, finding links inside bodyContent div element. */
public class WikiParser extends DefaultHandler {
	
	// Keep only html filenames ending relative paths and not containing tilde (~).
	private static Pattern linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	
	/** List of linked pages; filled by parser. */
	private List<Text> linkPageNames;
	/** Nesting depth inside bodyContent div element. */
	private int count = 0;

	public WikiParser(List<Text> linkPageNames) {
		super();
		this.linkPageNames = linkPageNames;
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		super.startElement(uri, localName, qName, attributes);
		if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
			// Beginning of bodyContent div element.
			count = 1;
		} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
			// Anchor tag inside bodyContent div element.
			count++;
			String link = attributes.getValue("href");
			if (link == null) {
				return;
			}
			try {
				// Decode escaped characters in URL.
				link = URLDecoder.decode(link, "UTF-8");
			} catch (Exception e) {
				// Wiki-weirdness; use link as is.
			}
			// Keep only html filenames ending relative paths and not containing tilde (~).
			Matcher matcher = linkPattern.matcher(link);
			if (matcher.find()) {
				String outlink = matcher.group(1);
				linkPageNames.add(new Text(outlink));
			}
		} else if (count > 0) {
			// Other element inside bodyContent div.
			count++;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		super.endElement(uri, localName, qName);
		if (count > 0) {
			// End of element inside bodyContent div.
			count--;
		}
	}
}
