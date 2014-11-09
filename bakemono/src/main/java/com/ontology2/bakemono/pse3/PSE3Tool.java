package com.ontology2.bakemono.pse3;

import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.MainBase.IncorrectUsageException;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.jena.PartitionOnSubject;
import com.ontology2.bakemono.jena.RawTripleComparator;
import com.ontology2.bakemono.jena.SPOTripleOutputFormat;
import com.ontology2.bakemono.jena.SubjectTripleComparator;
import com.ontology2.bakemono.mapreduce.SelfAwareTool;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

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
