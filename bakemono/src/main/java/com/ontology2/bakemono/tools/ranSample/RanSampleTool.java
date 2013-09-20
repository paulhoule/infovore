package com.ontology2.bakemono.tools.ranSample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.Main.IncorrectUsageException;
import com.ontology2.bakemono.sieve3.Sieve3Mapper;
import com.ontology2.bakemono.sieve3.Sieve3Tool;
import com.ontology2.bakemono.tools.pse3Tool.PSE3Tool;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class RanSampleTool implements Tool{
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
    public int run(String[] args) throws Exception {
        try {
            PeekingIterator<String> a=Iterators.peekingIterator(Iterators.forArray(args)); 
            
            Integer reduceTasks = PSE3Tool.parseRArgument(a);
            
            if (!a.hasNext())
                usage();

            double p=Double.parseDouble(a.next());
            conf.set(RanSampleMapper.P,Double.toString(p));

            if (!a.hasNext())
                usage();
            Path input=new Path(a.next());

            if (!a.hasNext())
                usage();
            Path output=new Path(a.next());
            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.output.compression.type", "BLOCK"); 
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

            conf.set(RanSampleMapper.NULL_VALUE, Boolean.toString((reduceTasks==null || reduceTasks==0)));
            Job job=new Job(conf,"sieve3");
            FileInputFormat.addInputPath(job, input);
            
            job.setJarByClass(RanSampleTool.class);
            job.setMapperClass(RanSampleMapper.class);

            FileOutputFormat.setOutputPath(job, output);
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            if(reduceTasks==null) {
                job.setNumReduceTasks(0);
            } else {
                job.setNumReduceTasks(reduceTasks);
                job.setReducerClass(PassthroughReducer.class);
            }
            return job.waitForCompletion(true) ? 0 :1;
        } catch(Main.IncorrectUsageException iue) {
            return 2;
        }
    }
    
    private static void usage() throws IncorrectUsageException {
        throw new Main.IncorrectUsageException("incorrect arguments");
    };

}
