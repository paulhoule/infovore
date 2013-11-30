package com.ontology2.bakemono.joins;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.configuration.HadoopTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

@HadoopTool("setDifference")
public class SetDifferenceTool implements Tool {
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

            String inputA=a.next();

            if (!a.hasNext())
                usage();

            String inputB=a.next();

            if (!a.hasNext())
                usage();

            String output=a.next();

            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.output.compression.type", "BLOCK");
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
            conf.set(SetJoinMapper.INPUTS+".1",inputA);
            conf.set(SetJoinMapper.INPUTS+".2",inputB);

            Job job=new Job(conf,"setDifference");
            job.setJarByClass(this.getClass());
            job.setMapperClass(TextSimpleJoinMapper.class);
            job.setReducerClass(SetDifferenceReducer.class);
            job.setGroupingComparatorClass(TaggedTextKeyGroupComparator.class);
            job.setPartitionerClass(TaggedKeyPartitioner.class);

            if(reduceTasks==null) {
                reduceTasks=1;    // about right for AWS runs
            }

            job.setNumReduceTasks(reduceTasks);

            job.setMapOutputKeyClass(TaggedTextItem.class);
            job.setMapOutputValueClass(VIntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            FileInputFormat.addInputPath(job, new Path(inputA));
            FileInputFormat.addInputPath(job, new Path(inputB));

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
