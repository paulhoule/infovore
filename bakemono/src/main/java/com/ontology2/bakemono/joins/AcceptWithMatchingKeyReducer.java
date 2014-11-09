package com.ontology2.bakemono.joins;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AcceptWithMatchingKeyReducer<K extends WritableComparable, V extends WritableComparable>
        extends Reducer<TaggedItem<K>, TaggedItem<V>, NullWritable, V> {

    final static VIntWritable ONE = new VIntWritable(1);
    final static VIntWritable TWO = new VIntWritable(2);

    @Override
    protected void reduce(TaggedItem<K> key, Iterable<TaggedItem<V>> values, Context context) throws IOException, InterruptedException {
        PeekingIterator<TaggedItem<V>> pi = Iterators.peekingIterator(values.iterator());
        boolean foundObject = false;

        while (pi.hasNext() && pi.peek().getTag().equals(ONE)) {
            pi.next();
            foundObject = true;
        }

        if(foundObject) {
            while (pi.hasNext()) {
                TaggedItem<V> that = pi.next();
                context.write(null, that.getKey());
            }
        }
    }
}
