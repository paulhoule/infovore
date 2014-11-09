package com.ontology2.bakemono.joins;


import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.configuration.HadoopTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.List;

@HadoopTool("fetchWithMatchingObjects3")
public class FetchTriplesWithMatchingObjectsTool implements Tool {
    private static Log logger= LogFactory.getLog(FetchTriplesWithMatchingObjectsTool.class);
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

            // The first argument is the list of objects
            String inputA=a.next();

            if (!a.hasNext())
                usage();

            // Middle positional parameters are sources of triples
            List<String> paths= Lists.newArrayList(a);

            // The last positional parameter is the output path
            String output=paths.get(paths.size() - 1);
            paths.remove(paths.size()-1);

            logger.info("Writing to output path "+output);
            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.output.compression.type", "BLOCK");
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
            conf.set(SetJoinMapper.INPUTS+".1",inputA);
            for(String path:paths)
                conf.set(SetJoinMapper.INPUTS+".2",path);

            Job job=new Job(conf,"fetchTriplesWithMatchingObjects");
            job.setJarByClass(this.getClass());
            job.setMapperClass(FetchTriplesWithMatchingObjectsMapper.class);
            job.setReducerClass(AcceptWithMatchingKeyReducer.class);
            job.setGroupingComparatorClass(TaggedTextKeyGroupComparator.class);
            job.setPartitionerClass(TaggedKeyPartitioner.class);

            if(reduceTasks==null) {
                reduceTasks=1;    // about right for AWS runs
            }

            job.setNumReduceTasks(reduceTasks);

            job.setMapOutputKeyClass(TaggedTextItem.class);
            job.setMapOutputValueClass(TaggedTextItem.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(inputA));
            for(String path:paths)
                FileInputFormat.addInputPath(job, new Path(path));

            FileOutputFormat.setOutputPath(job, new Path(output));
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
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
