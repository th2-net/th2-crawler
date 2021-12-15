package com.exactpro.th2.crawler;

public enum SessionAliasType {
    PLAIN_TEXT("PLAIN_TEXT"),
    REGEXP("REGEXP");

    private final String stringValue;

    SessionAliasType(String stringValue) {
        if (!stringValue.equals("PLAIN_TEXT") && !stringValue.equals("REGEXP")) {
            throw new IllegalArgumentException("SessionAliasType parameter must be either \"PLAIN_TEXT\" or \"REGEXP\"");
        }
        this.stringValue = stringValue;
    }

    public String getStringValue() {
        return stringValue;
    }
}