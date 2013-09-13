package com.ontology2.bakemono.sieve3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.Main.IncorrectUsageException;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;
import com.ontology2.bakemono.tools.pse3Tool.PSE3Tool;

/**
 * Sieve3Tool processes one Triple at a time,  meaning that it runs as a pure
 * Map without an associated map phase.
 * 
 * The is a general tool with the specialization being the configuration of a list of
 * (triplePredicate,hdfsPath) pairs that are processed as a series of rules.  In sequence,
 * the triple will be tested against every predicate and if it matches,  that triple will be sent to that path.
 * 
 * Once a triple matches a rule we are through with it and we move to the next triple;
 * if a triple matches no rules it falls out to the default output stream.  If the hdfsPath is null for
 * a matching predicate,  the system will ignore that triple entirely.
 * 
 *
 */
public class Sieve3Tool implements Tool {
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
        PeekingIterator<String> a=Iterators.peekingIterator(Iterators.forArray(arg0));    
        if (!a.hasNext())
            usage();
        
        String input=a.next();
        
        if (!a.hasNext())
            usage();
        
        String output=a.next();
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK"); 
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        Job job=new Job(conf,"pse3");
        job.setJarByClass(PSE3Tool.class);
        job.setMapperClass(Sieve3Mapper.class);
        job.setNumReduceTasks(0);
        return 0;
    }
    
    private static void usage() throws IncorrectUsageException {
        throw new Main.IncorrectUsageException("incorrect arguments");
    };
}
