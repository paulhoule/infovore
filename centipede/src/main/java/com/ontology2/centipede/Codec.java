package com.ontology2.centipede;

public interface Codec<T> {
    public String encode(T obj);
    public T decode(String obj);
}
