package com.ontology2.bakemono.diffFacts;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.ontology2.bakemono.joins.TaggedItem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

public class DiffFactReducer<KeyType extends WritableComparable>
        extends Reducer<TaggedItem<KeyType>,VIntWritable,Text,KeyType> {

    final static Logger log= Logger.getLogger(DiffFactReducer.class);
    final static VIntWritable ONE=new VIntWritable(1);
    final static VIntWritable TWO=new VIntWritable(2);

    final static Text A=new Text("A");
    final static Text D=new Text("D");

    @Override
    protected void reduce(TaggedItem<KeyType> key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        Set<VIntWritable> that= Sets.newHashSet();
        for(VIntWritable tag:values) {
            log.info("Saw key ["+key+"] with tag ["+tag+"]");
        }

        Iterables.addAll(that, values);

        if(that.contains(ONE) & !that.contains(TWO)) {
            context.write(D,key.getKey());
        }

        if(!that.contains(ONE) & that.contains(TWO)) {
            context.write(A,key.getKey());
        }
    }
}