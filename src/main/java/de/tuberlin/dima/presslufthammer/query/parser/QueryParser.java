package de.tuberlin.dima.presslufthammer.query.parser;

import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.query.Query;

/**
 * A simple helper to parse a query string to a {@link Query} object using the
 * ANTLR generated parser.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class QueryParser {
    public static Query parse(String queryString) throws ParseError {
        ANTLRStringStream input = new ANTLRStringStream(queryString);
        QLLexer lexer = new QLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        QLParser parser = new QLParser(tokens);
        try {
            Query query = parser.query();
            if (parser.getErrors().size() > 0) {
                throw new ParseError(parser.getErrors());
            }
            return query;
        } catch (RecognitionException e) {
            List<String> errors = Lists.newLinkedList();
            errors.add(e.getMessage());
            throw new ParseError(errors);
        }
    }

    public static class ParseError extends Exception {
        private static final long serialVersionUID = 1L;

        private List<String> errors;

        public ParseError(List<String> errors) {
            this.errors = errors;
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}
