package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

abstract public class GeneralTextJoinMapper extends GeneralJoinMapper<Text,Text> {
    @Override
    TaggedItem<Text> newTaggedKey(Text key, VIntWritable tag) {
        return new TaggedTextItem(key,tag);
    }

    @Override
    TaggedItem<Text> newTaggedValue(Text value, VIntWritable tag) {
        return new TaggedTextItem(value,tag);
    }
}
