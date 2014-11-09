package com.ontology2.bakemono.joins;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public abstract class SetJoinMapper<T extends WritableComparable>
        extends Mapper<LongWritable,T,TaggedItem<T>,VIntWritable> {

    static Logger log= Logger.getLogger(SetJoinMapper.class);
    public static final String JOINS="com.ontology2.bakemono.joins";
    public static final String INPUTS=JOINS+".inputs";
    static final Splitter dotSplitter= Splitter.on(".");
    static final Splitter commaSplitter= Splitter.on(",");

    Map<String,VIntWritable> mapping;

    //
    // We pass in the organization of the join as
    //
    // com.ontology2.bakemono.joins.inputs.1=path1,path2,path3
    // com.ontology2.bakemono.joins.inputs.2=path4
    //
    // where the paths are path prefixes;  anything that prefix
    // matches path1 will go into bucket 1 for the reducer,
    // anything that goes into bucket 2 will go into path4
    //

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration that=context.getConfiguration();
        mapping=getPathMapping(that);
    }

    static Map<String,VIntWritable> getPathMapping(Configuration that) {
        String prefixRegex=
                "^"+JOINS.replace(".","[.]")+".*$";
        Map<String,VIntWritable> mapping= Maps.newHashMap();

        Map<String,String> targets=that.getValByRegex(prefixRegex);
        for(String keyNumber:targets.keySet()) {
            VIntWritable i=new VIntWritable(Integer.parseInt(lastSegment(keyNumber)));
            for(String path:commaSplitter.split(targets.get(keyNumber)))
                mapping.put(path,i);
        }

        return mapping;
    }

    static String lastSegment(String input) {
        return Iterables.getLast(dotSplitter.split(input));
    }

    @Override
    protected void map(LongWritable key, T value, Context context) throws IOException, InterruptedException {
        FileSplit split=(FileSplit) context.getInputSplit();
        String thePath=split.getPath().toString();
        VIntWritable currentTag = determineTag(mapping,thePath);
        context.write(newTaggedKey(value,currentTag),currentTag);
    }

    static VIntWritable determineTag(Map<String,VIntWritable> mapping,String thePath) {
        VIntWritable currentTag=new VIntWritable(0);
        for(String aPrefix:mapping.keySet())
            if(thePath.startsWith(aPrefix))
                currentTag=mapping.get(aPrefix);

        return currentTag;
    }

    abstract TaggedItem<T> newTaggedKey(T key,VIntWritable tag);
}
