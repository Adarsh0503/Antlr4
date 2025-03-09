package com.example.sqlantlr.service;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class SqlFileParserService {

    // Pattern for DELIMITER statement
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^DELIMITER\\s+([^\\s]+)\\s*$", Pattern.CASE_INSENSITIVE);

    // Patterns for detecting CREATE PROCEDURE, FUNCTION, TRIGGER, or EVENT statements
    private static final Pattern CREATE_BLOCK_PATTERN = Pattern.compile("^\\s*CREATE\\s+(PROCEDURE|FUNCTION|TRIGGER|EVENT)\\s+", Pattern.CASE_INSENSITIVE);

    // Pattern for BEGIN keyword
    private static final Pattern BEGIN_PATTERN = Pattern.compile("\\bBEGIN\\b", Pattern.CASE_INSENSITIVE);

    // Pattern for END keyword followed by optional identifier and delimiter
    private static final Pattern END_PATTERN = Pattern.compile("\\bEND\\b(\\s+[A-Za-z0-9_]+)?", Pattern.CASE_INSENSITIVE);

    /**
     * Parse a SQL file into individual SQL queries, handling custom delimiters
     * and properly identifying stored procedure/function blocks
     *
     * @param file The uploaded SQL file
     * @return A list of SQL queries
     * @throws IOException If the file cannot be read
     */
    public List<String> parseFile(MultipartFile file) throws IOException {
        List<String> queries = new ArrayList<>();
        StringBuilder currentQuery = new StringBuilder();
        String currentDelimiter = ";";
        boolean inBlockDefinition = false;
        int beginBlockCount = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty()) {
                    // Still add the newline to preserve formatting
                    if (currentQuery.length() > 0) {
                        currentQuery.append("\n");
                    }
                    continue;
                }

                // Check for comments
                if (trimmedLine.startsWith("--") || trimmedLine.startsWith("#")) {
                    // Add comment to current query to preserve them
                    currentQuery.append(line).append("\n");
                    continue;
                }

                // Check for multi-line comments
                if (trimmedLine.startsWith("/*") && !trimmedLine.contains("*/")) {
                    currentQuery.append(line).append("\n");
                    continue;
                }

                // Check for DELIMITER statements
                Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(trimmedLine);
                if (delimiterMatcher.matches()) {
                    // If we have a partial query, add it
                    if (currentQuery.length() > 0 && !inBlockDefinition) {
                        queries.add(currentQuery.toString().trim());
                        currentQuery = new StringBuilder();
                    }

                    // Add the DELIMITER statement as a separate query if not in a block
                    if (!inBlockDefinition) {
                        queries.add(trimmedLine);
                    } else {
                        currentQuery.append(line).append("\n");
                    }

                    // Update the current delimiter
                    currentDelimiter = delimiterMatcher.group(1);
                    continue;
                }

                // Add the line to the current query
                currentQuery.append(line).append("\n");

                // Check if this is the start of a block definition (CREATE PROCEDURE, etc.)
                if (!inBlockDefinition && CREATE_BLOCK_PATTERN.matcher(trimmedLine).find()) {
                    inBlockDefinition = true;
                }

                // Count BEGIN keywords
                if (inBlockDefinition) {
                    Matcher beginMatcher = BEGIN_PATTERN.matcher(trimmedLine);
                    while (beginMatcher.find()) {
                        beginBlockCount++;
                    }

                    // Count END keywords
                    Matcher endMatcher = END_PATTERN.matcher(trimmedLine);
                    while (endMatcher.find()) {
                        beginBlockCount--;

                        // If we've reached the matching END for all BEGINs and the line ends with delimiter
                        if (beginBlockCount == 0 && trimmedLine.endsWith(currentDelimiter)) {
                            inBlockDefinition = false;
                            queries.add(currentQuery.toString().trim());
                            currentQuery = new StringBuilder();
                            break;
                        }
                    }
                }

                // For non-block statements, check if the line ends with the current delimiter
                if (!inBlockDefinition && trimmedLine.endsWith(currentDelimiter) && beginBlockCount == 0) {
                    // Add the complete query
                    queries.add(currentQuery.toString().trim());
                    currentQuery = new StringBuilder();
                }
            }

            // Add the last query if it doesn't end with a delimiter
            if (currentQuery.length() > 0) {
                queries.add(currentQuery.toString().trim());
            }
        }

        return queries;
    }
}