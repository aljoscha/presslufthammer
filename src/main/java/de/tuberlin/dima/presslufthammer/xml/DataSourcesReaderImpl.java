/**
 * 
 */
package de.tuberlin.dima.presslufthammer.xml;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author feichh
 * 
 */
public class DataSourcesReaderImpl extends DefaultHandler implements
		DataSourcesReader {

	public enum ParseState {
		START, DS, FIELD, TABLET,
	}

	private Logger log = Logger.getLogger(getClass());
	private Map<String, DataSource> dataSourceMap = null;
	private ParseState parseState = ParseState.DS;
	private DataSource currentSource = null;

	/**
	 * @param filename
	 * @return
	 */
	private static String convertToFileURL(String filename) {
		String path = new File(filename).getAbsolutePath();
		if (File.separatorChar != '/') {
			path = path.replace(File.separatorChar, '/');
		}
		if (!path.startsWith("/")) {
			path = "/" + path;
		}
		return "file:" + path;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.dima.presslufthammer.xml.DataSourcesReader#readFromXML(java
	 * .lang.String)
	 */
	public Map<String, DataSource> readFromXML(String path)
			throws ParserConfigurationException, SAXException, IOException {

		log.debug("attempting to parse " + path);
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(this);
		xmlReader.parse(convertToFileURL(path));

		return dataSourceMap;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.xml.sax.helpers.DefaultHandler#startDocument()
	 */
	@Override
	public void startDocument() throws SAXException {
		
		log.debug("document started");
		dataSourceMap = new HashMap<String, DataSource>();
		parseState = ParseState.START;
		currentSource = null;
		// super.startDocument();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String,
	 * java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		// TODO
		switch (parseState) {
		case START: {
			assert (localName == "dataSource");
			String name = attributes.getValue(0);
			String proto = attributes.getValue(1);
			currentSource = new DataSource(name, proto);
			dataSourceMap.put(name, currentSource);
			parseState = ParseState.DS;
			break;
		}
		case DS: {
			if (localName == "fields") {
				parseState = ParseState.FIELD;
			} else if( localName == "tablets") {
				parseState = ParseState.TABLET;
			}
			break;
		}
		case FIELD: {
			if (localName == "field") {
				String type = attributes.getValue(1);
				String name = attributes.getValue(0);
				currentSource.addField(name, type);
			}
			break;
		}
		case TABLET: {
			if( localName == "tablet") {
				String tablet = attributes.getValue(0);
				currentSource.addTablet(tablet);
			}
			break;
		}
		}
		super.startElement(uri, localName, qName, attributes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		// TODO
		switch (parseState) {
		case START:
			break;
		case DS:
			if (localName == "dataSource") {
				parseState = ParseState.START;
			}
			break;
		case FIELD:
			if (localName == "fields") {
				parseState = ParseState.DS;
			}
			break;
		case TABLET:
			if (localName == "tablets") {
				parseState = ParseState.DS;
			}
			break;
		}
		super.endElement(uri, localName, qName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.xml.sax.helpers.DefaultHandler#endDocument()
	 */
	@Override
	public void endDocument() throws SAXException {
		
		log.debug("document ended");
		super.endDocument();
	}

}
