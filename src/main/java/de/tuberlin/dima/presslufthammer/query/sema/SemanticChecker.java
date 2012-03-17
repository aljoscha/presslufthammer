package de.tuberlin.dima.presslufthammer.query.sema;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.util.Config.TableConfig;

/**
 * This class performs semantic analysis of a query and throw SemaError if
 * something is wrong. (In method checkQuery)
 * 
 * <p>
 * Checking is very rudimentary for now, but we always assume that we have
 * semantically correct queries after invoking this, so the system blows up if
 * we enter "malicious" queries ... :D
 * 
 * @author Aljoscha Krettek
 * 
 */
public class SemanticChecker {
    private final Logger log = LoggerFactory.getLogger(SemanticChecker.class);

    public boolean checkQuery(Query query, Map<String, TableConfig> tables)
            throws SemaError {
        String tableName = query.getTableName();
        if (!tables.containsKey(tableName)) {
            log.info("Table {} not in tables.", tableName);
            throw new SemaError("Table "+ tableName + " not available.");
        }
        return true;
    }
}
