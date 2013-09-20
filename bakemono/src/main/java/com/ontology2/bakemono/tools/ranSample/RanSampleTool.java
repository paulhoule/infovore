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

            Job job=new Job(conf,"sieve3");
            FileInputFormat.addInputPath(job, input);
            
            job.setJarByClass(RanSampleTool.class);
            job.setMapperClass(RanSampleMapper.class);
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, output);
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            return job.waitForCompletion(true) ? 0 :1;
        } catch(Main.IncorrectUsageException iue) {
            return 2;
        }
    }
    
    private static void usage() throws IncorrectUsageException {
        throw new Main.IncorrectUsageException("incorrect arguments");
    };

}
