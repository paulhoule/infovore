package com.ontology2.bakemono.abstractions;

public interface Codec<T> {
    public String encode(T obj);
    public T decode(String obj);
}
