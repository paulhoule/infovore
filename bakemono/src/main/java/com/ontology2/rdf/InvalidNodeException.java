package com.ontology2.rdf;

public class InvalidNodeException extends Exception {
    public InvalidNodeException() {
        this("");
    }
    public InvalidNodeException(String message) {
        super(message);
    }
}