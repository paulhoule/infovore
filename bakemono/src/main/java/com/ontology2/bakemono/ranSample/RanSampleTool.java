package com.ontology2.bakemono.ranSample;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.MainBase.IncorrectUsageException;
import com.ontology2.bakemono.configuration.HadoopTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

@HadoopTool("ranSample")
public class RanSampleTool implements Tool{
    public static Integer parseRArgument(PeekingIterator<String> a)
            throws IncorrectUsageException {
        Integer reduceTasks = null;
        while (a.hasNext() && a.peek().startsWith("-")) {
            String flagName = a.next().substring(1).intern();
            if (!a.hasNext())
                usage();

            String flagValue = a.next();
            if (flagName == "r") {
                reduceTasks = Integer.parseInt(flagValue);
            } else {
                usage();
            }
            ;
        }
        return reduceTasks;
    }

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
            
            Integer reduceTasks = parseRArgument(a);
            
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
            Job job=new Job(conf,"ranSample");
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
