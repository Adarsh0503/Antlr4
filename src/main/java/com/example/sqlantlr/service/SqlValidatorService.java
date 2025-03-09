package com.example.sqlantlr.service;

import com.sql.validator.MariaDBLexer;
import com.sql.validator.MariaDBParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
public class SqlValidatorService {

    public static class ValidationResult {
        private final boolean valid;
        private final String error;
        private final String query;
        private final int lineNumber;

        public ValidationResult(boolean valid, String error, String query, int lineNumber) {
            this.valid = valid;
            this.error = error;
            this.query = query;
            this.lineNumber = lineNumber;
        }

        public boolean isValid() {
            return valid;
        }

        public String getError() {
            return error;
        }

        public String getQuery() {
            return query;
        }

        public int getLineNumber() {
            return lineNumber;
        }
    }

    /**
     * Validates a list of SQL queries
     * @param queries List of SQL queries to validate
     * @return List of validation results
     */
    public List<ValidationResult> validateQueries(List<String> queries) {
        List<ValidationResult> results = new ArrayList<>();

        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i).trim();
            if (query.isEmpty()) {
                continue;
            }


            // Check if query ends with semicolon (unless it's a DELIMITER statement)
            if (!query.endsWith(";") && !query.toUpperCase().startsWith("DELIMITER ")) {
                results.add(new ValidationResult(
                        false,
                        "Missing semicolon at the end of the query",
                        query,
                        i + 1
                ));
                continue;
            }

            // Validate the individual query
            try {
                // Create the lexer
                MariaDBLexer lexer = new MariaDBLexer(CharStreams.fromString(query));
                // Create the token stream
                CommonTokenStream tokens = new CommonTokenStream(lexer);
                // Create the parser
                MariaDBParser parser = new MariaDBParser(tokens);
                // Configure error handling
                SyntaxErrorListener errorListener = new SyntaxErrorListener();
                parser.removeErrorListeners();
                parser.addErrorListener(errorListener);
                // Parse the query
                parser.root();

                if (errorListener.hasErrors()) {
                    results.add(new ValidationResult(false, errorListener.getErrorMessages(), query, i + 1));
                } else {
                    results.add(new ValidationResult(true, null, query, i + 1));
                }
            } catch (ParseCancellationException | RecognitionException e) {
                results.add(new ValidationResult(false, e.getMessage(), query, i + 1));
            } catch (Exception e) {
                results.add(new ValidationResult(false, "Unexpected error: " + e.getMessage(), query, i + 1));
            }
        }

        return results;
    }
}