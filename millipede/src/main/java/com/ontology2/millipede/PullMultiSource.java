package com.ontology2.millipede;

import com.ontology2.millipede.source.Source;

public interface PullMultiSource<T> extends MultiSource<T> {
    public Source<T> createSource(int binNumber) throws Exception;
}
