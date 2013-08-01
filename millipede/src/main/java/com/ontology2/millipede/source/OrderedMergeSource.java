package com.ontology2.millipede.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.ontology2.millipede.LineMultiFile;

public class OrderedMergeSource<T> implements Source<T> {

    class Item implements Comparable<Item> {
        public final Source<T> source;
        public final T value;

        public Item(Source<T> source) throws Exception {
            this.source = source;
            this.value = source.nextElement();
        }

        @Override
        public int compareTo(Item that) {
            return comparisonFunction.compare(this.value,that.value);
        }
    };

    final Comparator<T> comparisonFunction;
    final PriorityQueue<Item> items;

    public OrderedMergeSource(Collection<Source<T>> sources,Comparator<T> comparisonFunction) throws Exception {
        this.comparisonFunction=comparisonFunction;
        this.items=fillPriorityQueue(sources);
    }

    private PriorityQueue<Item> fillPriorityQueue(Collection<Source<T>> sources) throws Exception {
        PriorityQueue<Item> items=new PriorityQueue<Item>(sources.size());
        for(Source<T> s: sources) {
            if(s.hasMoreElements()) {
                items.add(new Item(s));
            }
        }

        return items;	
    }


    @Override
    public boolean hasMoreElements() {
        return (!items.isEmpty());
    }

    @Override
    public T nextElement() throws Exception {
        Item item=items.poll();
        if(item.source.hasMoreElements()) {
            items.add(new Item(item.source));
        }

        return item.value;
    }

    public static <S> OrderedMergeSource<S> fromMultiFile(LineMultiFile<S> mf,Comparator<S> comparisonFunction) throws Exception {
        List sources=new ArrayList(mf.getPartitionFunction().getPartitionCount());

        for (int i=0;i<mf.getPartitionFunction().getPartitionCount();i++) {
            sources.add(mf.createSource(i));
        }

        return new OrderedMergeSource<S>(sources, comparisonFunction);
    }


    public static <S extends Comparable<S>>  Source<S> fromMultiFile(
            LineMultiFile<S> in) throws Exception {
        return fromMultiFile(in,new NaturalOrdering<S>());
    };

}
