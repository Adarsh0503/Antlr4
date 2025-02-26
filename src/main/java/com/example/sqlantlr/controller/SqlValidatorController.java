package com.example.sqlantlr.controller;

import com.example.sqlantlr.service.SqlFileParserService;
import com.example.sqlantlr.service.SqlValidatorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/sql")
public class SqlValidatorController {

    private final SqlValidatorService validatorService;
    private final SqlFileParserService fileParserService;

    @Autowired
    public SqlValidatorController(SqlValidatorService validatorService, SqlFileParserService fileParserService) {
        this.validatorService = validatorService;
        this.fileParserService = fileParserService;
    }

    /**
     * Endpoint to validate a SQL query passed as a string
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateQuery(@RequestBody String query) {
        List<SqlValidatorService.ValidationResult> results = validatorService.validateQueries(List.of(query));
        return createResponse(results);
    }

    /**
     * Endpoint to validate a SQL file
     */
    @PostMapping("/validate-file")
    public ResponseEntity<Map<String, Object>> validateFile(@RequestParam("file") MultipartFile file) {
        try {
            List<String> queries = fileParserService.parseFile(file);
            List<SqlValidatorService.ValidationResult> results = validatorService.validateQueries(queries);
            return createResponse(results);
        } catch (IOException e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", "Failed to read file: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
        }
    }

    private ResponseEntity<Map<String, Object>> createResponse(List<SqlValidatorService.ValidationResult> results) {
        Map<String, Object> response = new HashMap<>();
        boolean allValid = results.stream().allMatch(SqlValidatorService.ValidationResult::isValid);

        response.put("success", allValid);
        response.put("totalQueries", results.size());
        response.put("validQueries", results.stream().filter(SqlValidatorService.ValidationResult::isValid).count());

        List<Map<String, Object>> detailedResults = results.stream()
                .map(result -> {
                    Map<String, Object> detail = new HashMap<>();
                    detail.put("lineNumber", result.getLineNumber());
                    detail.put("query", result.getQuery());
                    detail.put("valid", result.isValid());
                    if (!result.isValid()) {
                        detail.put("error", result.getError());
                    }
                    return detail;
                })
                .collect(Collectors.toList());

        response.put("results", detailedResults);

        return new ResponseEntity<>(response, allValid ? HttpStatus.OK : HttpStatus.BAD_REQUEST);
    }
}