package com.ontology2.millipede;

public interface Codec<T> {
    public String encode(T obj);
    public T decode(String obj);
}
