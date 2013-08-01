package com.ontology2.rdf;

public interface Economizer<T> {
    /**
     * @param that an instance of T
     * @return some instance of T for which economize(that).equals(that)
     */
    public T economize(T that);
}
