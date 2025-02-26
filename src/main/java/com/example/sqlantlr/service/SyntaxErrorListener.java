package com.example.sqlantlr.service;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;

public class SyntaxErrorListener extends BaseErrorListener {
    private final List<String> errors = new ArrayList<>();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        String errorMsg = String.format("line %d:%d %s", line, charPositionInLine, msg);
        errors.add(errorMsg);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public String getErrorMessages() {
        return String.join("; ", errors);
    }

    public List<String> getErrors() {
        return errors;
    }
}