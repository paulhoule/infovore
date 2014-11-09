package com.ontology2.bakemono.joins;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

//
// How this is used:
//
// Set Members are of <Type>,  and the identity of the set is encoded as a
// VIntWritable,  which is either 1 or 2.
//
// We're performing the set substraction S_1 - S_2,  so we are fetching elements
// of set one that are not members of set two.
//
//

public class SetDifferenceReducer<KeyType extends WritableComparable>
        extends Reducer<TaggedItem<KeyType>,VIntWritable,KeyType,NullWritable> {

    @Override
    protected void reduce(TaggedItem<KeyType> key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        Set<Integer> that= Sets.newHashSet();
        for(VIntWritable tag:values)
            that.add(tag.get());

        if(that.contains(1) & !that.contains(2)) {
            context.write(key.getKey(),null);
        }
    }
}
