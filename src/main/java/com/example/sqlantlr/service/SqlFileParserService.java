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
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^DELIMITER\\s+([^\\s;]+)\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * Parse a SQL file into individual SQL queries, handling custom delimiters
     *
     * @param file The uploaded SQL file
     * @return A list of SQL queries
     * @throws IOException If the file cannot be read
     */
    public List<String> parseFile(MultipartFile file) throws IOException {
        List<String> queries = new ArrayList<>();
        StringBuilder currentQuery = new StringBuilder();
        String currentDelimiter = ";";

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // Check for comments
                if (line.startsWith("--") || line.startsWith("#")) {
                    // Add comment to current query to preserve them
                    currentQuery.append(line).append("\n");
                    continue;
                }

                // Check for DELIMITER statements
                Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(line);
                if (delimiterMatcher.matches()) {
                    // If we have a partial query, add it
                    if (currentQuery.length() > 0) {
                        queries.add(currentQuery.toString().trim());
                        currentQuery = new StringBuilder();
                    }

                    // Add the DELIMITER statement as a separate query
                    queries.add(line);

                    // Update the current delimiter
                    currentDelimiter = delimiterMatcher.group(1);
                    continue;
                }

                // Add the line to the current query
                currentQuery.append(line).append("\n");

                // Check if the line ends with the current delimiter
                if (line.endsWith(currentDelimiter)) {
                    // Remove delimiter from the end to avoid confusion in the parser
                    String query = currentQuery.toString().trim();
                    queries.add(query);
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