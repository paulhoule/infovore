package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedKeyGroupComparator<T extends WritableComparable>
        extends WritableComparator {
    public TaggedKeyGroupComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b) {
        TaggedKey<T> left=(TaggedKey<T>) a;
        TaggedKey<T> right=(TaggedKey<T>) b;
        return left.getKey().compareTo(right.getKey());
    }
}
