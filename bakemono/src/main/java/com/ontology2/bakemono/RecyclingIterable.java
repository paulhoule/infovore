package com.ontology2.bakemono;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Iterator;

/**
 *
 * The Recycling Iterable exists for use in testing to provoke problems that could occur
 * when running inside Hadoop.  The Recycling Iterable rejects any attempt to rewind an
 * Iterable by calling iterator() more than once and also reuses the same Writable object
 * over and over again as does Hadoop.
 *
 * @param <T> The type of the iterable,  which extends Writable
 */
public class RecyclingIterable<T extends Writable> implements Iterable<T> {

    private final Iterable<T> that;
    private final Class<T> type;

    public RecyclingIterable(Class<T> type, Iterable<T> that) {
        this.that=that;
        this.type=type;
    }

    public RecyclingIterable(Class<T> type, T... items) {
        this.that= Lists.newArrayList(items);
        this.type=type;
    }
    
    int callCount=0;

    public Iterator<T> iterator() {
        if(callCount>0) {
            throw new UnsupportedOperationException("iterator() was called more than once");
        }
        callCount++;

        return new Iterator<T>() {
            final Iterator<T> innerIterator;
            final T item;

            {
                innerIterator=that.iterator();
                try {
                    item=(T) type.newInstance();
                } catch (InstantiationException|IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }


            @Override
            public boolean hasNext() {
                return innerIterator.hasNext();
            }

            @Override
            public T next() {
                T next=innerIterator.next();
                ByteArrayOutputStream store=new ByteArrayOutputStream();
                DataOutput out=new DataOutputStream(store);
                try {
                    next.write(out);
                    DataInput in=new DataInputStream(new ByteArrayInputStream(store.toByteArray()));
                    item.readFields(in);
                    return item;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Can't remove from a gimpy iterable");
            }
        };
    }
}

