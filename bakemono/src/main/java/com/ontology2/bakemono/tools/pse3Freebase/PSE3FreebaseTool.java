package com.ontology2.bakemono.tools.pse3Freebase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.reasoner.rulesys.impl.TempNodeCache.NodePair;
import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.jena.SPOTripleOutputFormat;
import com.ontology2.bakemono.jena.STripleOutputFormat;
import com.ontology2.bakemono.jena.TripleComparator;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;
import com.ontology2.bakemono.mappers.pse3Freebase.PSE3FreebaseMapper;
import com.ontology2.bakemono.reducers.uniq.Uniq;

public class PSE3FreebaseTool implements Tool {

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
            if(arg0.length!=2)
                Main.errorCausedByUser("You must specify both input and output paths");
    
            String input=arg0[0];
            String output=arg0[1];
            
            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.output.compression.type", "BLOCK"); 
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
            Job job=new Job(conf,"pse3");
            job.setJarByClass(PSE3FreebaseTool.class);
            job.setMapperClass(PSE3FreebaseMapper.class);   // no ChainMapoper for new API in Hadoop 1,  no cookie for y0u
            job.setReducerClass(Uniq.class);
            job.setNumReduceTasks(500);
            
            job.setMapOutputKeyClass(WritableTriple.class);
            job.setMapOutputValueClass(LongWritable.class);
            
            job.setOutputFormatClass(SPOTripleOutputFormat.class);
            job.setOutputKeyClass(Triple.class);
            job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            return job.waitForCompletion(true) ? 0 :1;
        } catch(Main.IncorrectUsageException iue) {
            return 2;
        }
    }

}