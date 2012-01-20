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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author feichh
 * 
 */
public class DataSourcesReaderImpl extends DefaultHandler implements
		DataSourcesReader {

	public enum ParseState {
		START, DS,
	}

	private Logger log = LoggerFactory.getLogger(getClass());
	private Map<String, DataSource> dataSourceMap = null;
	private ParseState parseState = ParseState.DS;
	private DataSource currentSource = null;

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

	@Override
	public void startDocument() throws SAXException {

		log.debug("document started");
		dataSourceMap = new HashMap<String, DataSource>();
		parseState = ParseState.START;
		currentSource = null;
		// super.startDocument();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		// TODO
		switch (parseState) {
		case START: {
			assert (localName == "dataSource");
			try {
				String name = attributes.getValue(0);
				String path = attributes.getValue(1);
				int parts = Integer.valueOf(attributes.getValue(2));
				currentSource = new DataSource(path, parts);
				dataSourceMap.put(name, currentSource);
				parseState = ParseState.DS;
			} catch (Exception e) {
				throw new SAXException(e);
			}
			break;
		}
		case DS: {
			// could be considered irrelevant
			throw new SAXParseException("XML Structure: nested <dataSource>s",
					null);
		}
		}
		// super.startElement(uri, localName, qName, attributes);
	}

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
		}
		// super.endElement(uri, localName, qName);
	}

	@Override
	public void endDocument() throws SAXException {

		log.debug("document ended");
		super.endDocument();
	}

}
