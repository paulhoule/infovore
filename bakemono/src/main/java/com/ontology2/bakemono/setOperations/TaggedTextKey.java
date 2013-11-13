package com.ontology2.bakemono.setOperations;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class TaggedTextKey extends TaggedKey<Text> {

    public TaggedTextKey() { super(); }
    public TaggedTextKey(Text key,VIntWritable tag) {
       super(key,tag);
    }

    @Override
    protected Text newT() {
        return new Text();
    }
}
