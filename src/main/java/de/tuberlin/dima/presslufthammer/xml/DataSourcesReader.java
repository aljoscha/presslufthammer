package de.tuberlin.dima.presslufthammer.xml;

import java.util.Map;

public interface DataSourcesReader {

	public Map<String, DataSource> readFromXML(String path) throws Exception;
}
