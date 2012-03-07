package de.tuberlin.dima.presslufthammer.query.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import de.tuberlin.dima.presslufthammer.query.Query;

/**
 * A simple helper to parse a query string to a {@link Query} object using the ANTLR
 * generated parser.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class QueryParser {
    public static Query parse(String queryString) {
        ANTLRStringStream input = new ANTLRStringStream(queryString);
        QLLexer lexer = new QLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        QLParser parser = new QLParser(tokens);
        try {
            return parser.query();
        } catch (RecognitionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
