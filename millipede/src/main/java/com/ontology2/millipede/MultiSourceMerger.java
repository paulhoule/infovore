package com.ontology2.millipede;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.OrderedMergeSource;
import com.ontology2.millipede.source.Source;

public class MultiSourceMerger<T> implements MultiSource<T> {

    private final ImmutableList<PullMultiSource<T>> sources;
    private final Comparator<T> comparisonFunction;

    public MultiSourceMerger(Collection<PullMultiSource<T>> sources,Comparator<T> comparisonFunction) {
        this.sources=ImmutableList.copyOf(sources);
        this.comparisonFunction=comparisonFunction;
    }

    public int getPartitionCount() {
        return sources.get(0).getPartitionCount();
    }

    @Override
    public long pushBin(int binNumber, Sink<T> destination) throws Exception {
        List<Source<T>> innerSources=Lists.newArrayList();
        for(PullMultiSource<T> s:sources) {
            innerSources.add(s.createSource(binNumber));
        }
        Source<T> mergeSource=new OrderedMergeSource(innerSources,comparisonFunction);
        return Plumbing.flow(mergeSource,destination);
    }

}
