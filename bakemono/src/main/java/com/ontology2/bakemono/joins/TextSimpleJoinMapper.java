package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class TextSimpleJoinMapper extends SimpleJoinMapper<Text> {
    @Override
    TaggedKey<Text> newTaggedKey(Text key, VIntWritable tag) {
        return new TaggedTextKey(key,tag);
    }
}
