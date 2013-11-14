package com.ontology2.bakemono.joins;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.expression.spel.ast.OpNE;

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
public class SetDifferenceReducer<Type extends WritableComparable>
        extends Reducer<TaggedKey<Type>,VIntWritable,Type,NullWritable> {

    final static VIntWritable ONE=new VIntWritable(1);
    final static VIntWritable TWO=new VIntWritable(2);

    @Override
    protected void reduce(TaggedKey<Type> key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        Set<VIntWritable> that= Sets.newHashSet();
        Iterables.addAll(that,values);
        String newKey=key.getKey()+" "+that.toString();
        if(that.contains(ONE) & !that.contains(TWO)) {
            context.write(key.getKey(),null);
        }
    }
}
