package com.ontology2.bakemono.mapmap;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.uniq.Uniq;
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

import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/12/13
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */

public abstract class UniqTool implements Tool {

    abstract protected Class getMapperClass();
    abstract protected String getJobName();

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
    public int run(String[] arg0) throws Exception {
        try {
            PeekingIterator<String> a= Iterators.peekingIterator(Iterators.forArray(arg0));
            Integer reduceTasks = parseRArgument(a);

            if (!a.hasNext())
                usage();

            List<String> paths= Lists.newArrayList(a);

            String output=paths.get(paths.size()-1);
            paths.remove(paths.size()-1);

            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.output.compression.type", "BLOCK");
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

            Job job=new Job(conf,getJobName());
            job.setSpeculativeExecution(false);
            job.setJarByClass(this.getClass());
            job.setMapperClass(getMapperClass());
            job.setReducerClass(Uniq.class);

            if(reduceTasks==null) {
                reduceTasks=29;    // about right for AWS runs
            }

            job.setNumReduceTasks(reduceTasks);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            for(String input:paths) {
                FileInputFormat.addInputPath(job, new Path(input));
            }

            FileOutputFormat.setOutputPath(job, new Path(output));
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

            // Gotcha -- this has to run before the definitions above associated with the output format because
            // this is going to be configured against the job as it stands a moment from now

            job.setOutputFormatClass(TextOutputFormat.class);

            return job.waitForCompletion(true) ? 0 : 1;
        } catch(Main.IncorrectUsageException iue) {
            return 2;
        }
    }

    public static Integer parseRArgument(PeekingIterator<String> a)
            throws Main.IncorrectUsageException {
        Integer reduceTasks=null;
        while(a.hasNext() && a.peek().startsWith("-")) {
            String flagName=a.next().substring(1).intern();
            if (!a.hasNext())
                usage();

            String flagValue=a.next();
            if (flagName=="r") {
                reduceTasks=Integer.parseInt(flagValue);
            } else {
                usage();
            };
        }
        return reduceTasks;
    }

    private static void usage() throws Main.IncorrectUsageException {
        throw new Main.IncorrectUsageException("incorrect arguments");
    };

}
