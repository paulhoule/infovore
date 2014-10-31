package com.ontology2.bakemono.pse3;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.MainBase.IncorrectUsageException;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.jena.*;
import com.ontology2.bakemono.joins.TaggedItem;
import com.ontology2.bakemono.joins.TaggedKeyPartitioner;
import com.ontology2.bakemono.joins.TaggedTextKeyGroupComparator;
import com.ontology2.bakemono.joins.TaggedTextKeySortComparator;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@HadoopTool("pse3")
public class PSE3Tool extends SelfAwareTool<PSE3Options> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(PSE3Tool.class);

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return SubjectHashedUniq.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return SPOTripleOutputFormat.class;
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
