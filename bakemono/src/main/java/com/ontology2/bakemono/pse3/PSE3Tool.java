package com.ontology2.bakemono.pse3;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.MainBase.IncorrectUsageException;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.jena.*;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@HadoopTool("pse3")
public class PSE3Tool extends SelfAwareTool<PSE3Options> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(PSE3Tool.class);


    private Configuration conf;

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration arg0) {
        this.conf = arg0;
    }

    @Override
    public int run(String[] arg0) throws Exception {
        logger.warn("Entering PSE3Tool run() method");

        try {
            PeekingIterator<String> a = Iterators.peekingIterator(Iterators.forArray(arg0));
            Integer reduceTasks = parseRArgument(a);

            if (!a.hasNext())
                usage();

            String input = a.next();

            if (!a.hasNext())
                usage();

            String output = a.next();

            Path acceptedPath = new Path(output, "accepted");           ;

            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.map.max.attempts","1");
            conf.set("mapred.output.compression.type", "BLOCK");
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

            Job job = new Job(conf, "pse3");
            job.setReduceSpeculativeExecution(false);
            job.setMapSpeculativeExecution(false);
            job.setJarByClass(PSE3Tool.class);
            job.setMapperClass(PSE3Mapper.class);
            job.setReducerClass(SubjectHashedUniq.class);

            if (reduceTasks == null) {
                reduceTasks = 29;    // about right for AWS runs
            }

            job.setNumReduceTasks(reduceTasks);

            job.setMapOutputKeyClass(WritableTriple.class);
            job.setMapOutputValueClass(WritableTriple.class);
            job.setOutputKeyClass(Triple.class);
            job.setOutputValueClass(LongWritable.class);
            job.setGroupingComparatorClass(getGroupingComparatorClass());
            job.setSortComparatorClass(getSortComparatorClass());
            Class<? extends Partitioner> p=getPartitionerClass();
            job.setPartitionerClass(p);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, acceptedPath);
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

            // Gotcha -- this has to run before the definitions above associated with the output format because
            // this is going to be configured against the job as it stands a moment from now

            job.setOutputFormatClass(SPOTripleOutputFormat.class);

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Main.IncorrectUsageException iue) {
            return 2;
        }
    }

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

    private static void usage() throws IncorrectUsageException {
        throw new Main.IncorrectUsageException("incorrect arguments");
    }


    @Override
    public Class<? extends RawComparator> getGroupingComparatorClass() {
        return SubjectTripleComparator.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return PartitionOnSubject.class;
    }

    @Override
    public Class<? extends RawComparator> getSortComparatorClass() {
        return RawTripleComparator.class;
    }
}
