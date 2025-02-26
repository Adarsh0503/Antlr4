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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class SqlValidatorService {

    // Pattern for DELIMITER statement
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^DELIMITER\\s+([^\\s;]+)\\s*$", Pattern.CASE_INSENSITIVE);

    // Pattern to detect start of procedure/function body
    private static final Pattern PROCEDURE_BODY_START = Pattern.compile("\\b(CREATE|ALTER)\\s+(PROCEDURE|FUNCTION|TRIGGER|EVENT)\\b", Pattern.CASE_INSENSITIVE);

    // Pattern to detect end of procedure/function body with custom delimiter
    private static final Pattern PROCEDURE_BODY_END = Pattern.compile("\\bEND\\b");

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

        String currentDelimiter = ";";
        boolean inProcedureBody = false;
        StringBuilder procedureBodyBuilder = null;
        int procedureStartLine = -1;

        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i).trim();
            if (query.isEmpty()) {
                continue;
            }

            // Check for DELIMITER statement
            Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(query);
            if (delimiterMatcher.matches()) {
                currentDelimiter = delimiterMatcher.group(1);
                results.add(new ValidationResult(true, null, query, i + 1));
                continue;
            }

            // If we're in procedure body, collect statements until we find the delimiter
            if (inProcedureBody) {
                procedureBodyBuilder.append(query).append("\n");

                // Check if we've reached the end of the procedure body
                if (query.endsWith(currentDelimiter)) {
                    String withoutDelimiter = query.substring(0, query.length() - currentDelimiter.length());
                    Matcher endMatcher = PROCEDURE_BODY_END.matcher(withoutDelimiter);
                    if (endMatcher.find()) {
                        // We've found the end of the procedure body
                        inProcedureBody = false;
                        String fullProcedureBody = procedureBodyBuilder.toString();

                        // Validate the full procedure body
                        try {
                            validateRoutineBody(fullProcedureBody);
                            results.add(new ValidationResult(true, null, fullProcedureBody, procedureStartLine));
                        } catch (Exception e) {
                            results.add(new ValidationResult(false, e.getMessage(), fullProcedureBody, procedureStartLine));
                        }

                        procedureBodyBuilder = null;
                    }
                }
                continue;
            }

            // Check if this starts a procedure body
            Matcher procedureStartMatcher = PROCEDURE_BODY_START.matcher(query);
            if (procedureStartMatcher.find()) {
                // This might be the start of a procedure body
                // Check if it's a complete statement or just the beginning
                if (!query.endsWith(currentDelimiter)) {
                    // This is just the beginning of a procedure body
                    inProcedureBody = true;
                    procedureBodyBuilder = new StringBuilder(query).append("\n");
                    procedureStartLine = i + 1;
                    continue;
                }
            }

            // If we reach here, this is a normal SQL statement
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

        // If we're still in a procedure body at the end, it's incomplete
        if (inProcedureBody) {
            String incompleteProcedure = procedureBodyBuilder.toString();
            results.add(new ValidationResult(false, "Incomplete procedure definition", incompleteProcedure, procedureStartLine));
        }

        return results;
    }

    /**
     * Validates a routine body
     * This requires special handling because routine bodies can contain multiple statements
     */
    private void validateRoutineBody(String routineBody) throws Exception {
        // Create the lexer
        MariaDBLexer lexer = new MariaDBLexer(CharStreams.fromString(routineBody));

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
            throw new Exception(errorListener.getErrorMessages());
        }
    }
}