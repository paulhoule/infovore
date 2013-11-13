package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/13/13
 * Time: 2:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class TextSimpleJoinMapper extends SimpleJoinMapper<Text> {
    @Override
    TaggedKey<Text> newTaggedKey(Text key, VIntWritable tag) {
        return new TaggedTextKey(key,tag);
    }
}
