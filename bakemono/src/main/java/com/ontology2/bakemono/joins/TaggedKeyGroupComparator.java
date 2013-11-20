package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedKeyGroupComparator<T extends WritableComparable>
        extends WritableComparator {

    //
    // Because of type erasure,  a TaggedItem doesn't know how to instantiate
    // itself,  so you need to use a subclass of TaggedItem.  This class doesn't
    // know which subclass it needs to instantiate so you must subclass this to
    // make something that can self-instantiate
    //
    protected TaggedKeyGroupComparator(Class childClass) {
        super(childClass,true);
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b) {
        TaggedItem<T> left=(TaggedItem<T>) a;
        TaggedItem<T> right=(TaggedItem<T>) b;
        return left.getKey().compareTo(right.getKey());
    }
}
