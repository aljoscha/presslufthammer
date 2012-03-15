/**
 * 
 */
package scratch;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.xml.DataSource;
import de.tuberlin.dima.presslufthammer.xml.DataSourcesReader;
import de.tuberlin.dima.presslufthammer.xml.DataSourcesReaderImpl;

/**
 * A test for the XMLParser for the DataSources file DataSourcesReader.
 * 
 * @author feichh
 * 
 */
public class DataSourcesReaderTest {
	/**
	 * The logging logger.
	 */
	private static final Logger log = LoggerFactory
			.getLogger("DataSourcesReaderTest");
	/**
	 * Map for the parse result.
	 */
	private static Map<String, DataSource> tables;
	/**
	 * Default path to DataSources.xml file.
	 */
	private static final String DATASOURCES = "src/main/example-data/DataSources.xml";

	/**
	 * @param args
	 *            optional first argument = path to DataSources.xml
	 */
	public static void main(String[] args) {
		DataSourcesReader dsReader = new DataSourcesReaderImpl();
		String path = DATASOURCES;
		if (args.length > 0) {
			path = args[0];
		}

		try {
			tables = dsReader.readFromXML(path);
			log.info("Read datasources from {}.", path);
			log.info(tables.toString());
		} catch (NullPointerException e) {
			log.error("NullPointer reading DataSources from {}.", path, e);
		} catch (Exception e) {
			log.error("Error reading datasources from {}.", path, e);
		}
		System.out.println("Parsing finished with the following result:");
		System.out.println(tables);
	}
}
