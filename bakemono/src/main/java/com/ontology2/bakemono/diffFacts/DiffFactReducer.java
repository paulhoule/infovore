package com.ontology2.bakemono.diffFacts;

import com.google.common.collect.Sets;
import com.ontology2.bakemono.joins.TaggedItem;
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
        Set<Integer> that= Sets.newHashSet();
        for(VIntWritable tag:values) {
            that.add(tag.get());
        }

        if(that.contains(1) & !that.contains(2)) {
            context.write(D,key.getKey());
        }

        if(!that.contains(1) & that.contains(2)) {
            context.write(A,key.getKey());
        }
    }
}