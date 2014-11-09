package com.ontology2.bakemono.diffFacts;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.joins.*;
import com.ontology2.centipede.errors.UsageException;
import com.ontology2.centipede.parser.OptionParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@HadoopTool("rdfDiff")
public class DiffFactsTool implements Tool {
    @Autowired ApplicationContext applicationContext;
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration arg0) {
        this.conf=arg0;
    }

    @Override
    public int run(String[] strings) throws Exception {
        OptionParser parser=new OptionParser(DiffFactsOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        DiffFactsOptions o= (DiffFactsOptions) parser.parse(Lists.newArrayList(strings));
        if (o.left.isEmpty())
            throw new UsageException("you did not specify a value for -left");

        if (o.right.isEmpty())
            throw new UsageException("you did not specify a value for -right");

        if (o.output.isEmpty())
            throw new UsageException("you did not specify a value for -output");

        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.set(SetJoinMapper.INPUTS+".1", Joiner.on(",").join(o.left));
        conf.set(SetJoinMapper.INPUTS+".2", Joiner.on(",").join(o.right));

        Job job=new Job(conf,"diffFacts");
        job.setJarByClass(this.getClass());
        job.setMapperClass(TextSimpleJoinMapper.class);
        job.setReducerClass(DiffFactReducer.class);
        job.setGroupingComparatorClass(TaggedTextKeyGroupComparator.class);
        job.setPartitionerClass(TaggedKeyPartitioner.class);

        if(o.reducerCount<1) {
            o.reducerCount=1;
        }

        job.setNumReduceTasks(o.reducerCount);

        job.setMapOutputKeyClass(TaggedTextItem.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for(String path: Iterables.concat(o.left, o.right)) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(o.output));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
