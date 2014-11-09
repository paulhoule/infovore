package com.ontology2.bakemono.mapreduce;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ontology2.bakemono.joins.GeneralJoinMapper;
import com.ontology2.bakemono.mapred.ToolBase;
import com.ontology2.centipede.parser.OptionParser;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;

public abstract class SingleJobTool<OptionsClass> extends ToolBase {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(SingleJobTool.class);

    protected OptionsClass options;
    protected void validateOptions() {} ;
    abstract protected String getName();
    protected Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }
    protected abstract Class<? extends Mapper> getMapperClass();

    protected abstract Class<? extends Writable> getMapOutputKeyClass();
    protected abstract Class<? extends Writable> getMapOutputValueClass();

    protected Class<? extends Reducer> getReducerClass() {
        return Reducer.class;
    }

    protected Class<? extends RawComparator> getGroupingComparatorClass() {
        return null;
    }

    protected Class<? extends Partitioner> getPartitionerClass() {
        return null;
    }

    protected Class<? extends RawComparator> getSortComparatorClass() {
        return null;
    }

    abstract public Class<? extends Writable> getOutputKeyClass();
    abstract public Class<? extends Writable> getOutputValueClass();

    abstract public Iterable<Path> getInputPaths();

    public Multimap<Integer,Path> getTagMap() {
        return HashMultimap.create();
    }

    abstract public int getNumReduceTasks();
    protected abstract Path getOutputPath();
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
    }

    //
    // "null" means don't compress
    //

    protected Class<? extends CompressionCodec> getOutputCompressorClass() {
        return GzipCodec.class;
    }

    //
    // the assumption here is that any real instance of this will be a non-generic
    // subclass of a class that has the parameter filled in,  thus we can figure
    // out the class from the parameter.
    //
    
    abstract public Class getOptionsClass();

    public int run(String[] strings) throws Exception {
        logger.info("Initializing SingleJobTool");
        Job job = createJob(strings);
        if(job.waitForCompletion(true))
            return 0;

        if(getErrorSleepTime()<1)
            return 1;

        logger.info("Waiting for ["+getErrorSleepTime()+"] seconds for logs to synchronize");
        Thread.sleep(1000*getErrorSleepTime());

        return 1;
    }

    //
    // this is public so it is accessible for testing...  that is,  we can test a dry run that
    // creates the job but doesn't run it
    //

    public Job createJob(String[] strings) throws IllegalAccessException, IOException {
        options=extractOptions(strings);
        validateOptions();

        configureOutputCompression();
        Job job=new Job(getConf(),getName());
        job.setJarByClass(getClass());
        job.setMapperClass(getMapperClass());
        job.setMapOutputKeyClass(getMapOutputKeyClass());
        job.setMapOutputValueClass(getMapOutputValueClass());
        job.setReducerClass(getReducerClass());
        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());

        if(getGroupingComparatorClass()!=null) {
            logger.info("Set grouping comparator class to "+getGroupingComparatorClass());
            job.setGroupingComparatorClass(getGroupingComparatorClass());
        }

        if(getPartitionerClass()!=null) {
            logger.info("Set partitioner class to "+getPartitionerClass());
            job.setPartitionerClass(getPartitionerClass());
        }

        if(getSortComparatorClass()!=null) {
            logger.info("Set sort comparator class to "+getSortComparatorClass());
            job.setSortComparatorClass(getSortComparatorClass());
        }

        job.setInputFormatClass(getInputFormatClass());
        for(Path p:getInputPaths()) {
            FileInputFormat.addInputPath(job, p);
        }

        Multimap<Integer,Path> tagMap=getTagMap();
        if(tagMap!=null && !tagMap.isEmpty()) {
            for(Integer key:tagMap.keySet()) {
                Iterable<Path> paths=tagMap.get(key);
                String configKey= GeneralJoinMapper.INPUTS+"."+key;
                String configValue=Joiner.on(",").join(paths);
                job.getConfiguration().set(configKey,configValue);
            }
        }

        job.setNumReduceTasks(getNumReduceTasks());
        FileOutputFormat.setOutputPath(job, getOutputPath());
        job.setOutputFormatClass(getOutputFormatClass());
        serializeOptions(job);

        // should we let output compression be configurable?  the bloom filters shouldn't be compressible
        // if they are optimally tuned,  but trying to compress a file that size won't hurt

        if(getOutputCompressorClass()!=null) {
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job,getOutputCompressorClass());
        }

        return job;
    }

    private void serializeOptions(Job job) throws IllegalAccessException {
        Class that=getOptionsClass();
        for(Field f:that.getFields()) {
            StoreAs a=f.getAnnotation(StoreAs.class);
            if (a!=null) {
                job.getConfiguration().set(a.value(),f.get(options).toString());
            }
        }
    }

    private OptionsClass extractOptions(String[] strings) throws IllegalAccessException {
        return extractOptions(Lists.newArrayList(strings));
    }

    private OptionsClass extractOptions(ArrayList<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(getOptionsClass());
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        return (OptionsClass) parser.parse(strings);
    }


    public int getErrorSleepTime() {
        return 600; // ten minutes,  since AWS log syncs up every 5 minutes
    }
}
