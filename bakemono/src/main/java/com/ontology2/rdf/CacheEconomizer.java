package com.ontology2.rdf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CacheEconomizer<T> implements Economizer<T> {

    final Cache<T,T> items=CacheBuilder.newBuilder().maximumSize(10000).build();

    @Override
    public T economize(final T that) {
        try {
            return items.get(that,new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return that;
                }});
        } catch(ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }


}
