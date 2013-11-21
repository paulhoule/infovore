package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class TaggedTextItem extends TaggedItem<Text> {

    public TaggedTextItem() { super(); }
    public TaggedTextItem(Text key, VIntWritable tag) {
        super(key,tag);
    }

    public TaggedTextItem(Text key, int tag) {
        this(key,new VIntWritable(tag));
    }

    public TaggedTextItem(String key, int tag) {
        this(new Text(key),tag);
    }

    @Override
    protected Text newT() {
        return new Text();
    }
}
