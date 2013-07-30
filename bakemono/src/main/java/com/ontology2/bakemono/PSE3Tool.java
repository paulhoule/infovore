package com.ontology2.bakemono;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import com.hp.hpl.jena.reasoner.rulesys.impl.TempNodeCache.NodePair;
import com.ontology2.bakemono.jena.STripleOutputFormat;

public class PSE3Tool implements Tool {

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
		if(arg0.length!=2)
			Main.errorCausedByUser("You must specify both input and output paths");
		
		String input=arg0[0];
		String output=arg0[1];
		
		conf.set("mapred.output.compress","true");
		conf.set("mapred.output.compression.type",CompressionType.BLOCK.toString());
		conf.set("mapred.output.compression.codec",GzipCodec.class.getCanonicalName());
		
		Job job=new Job(conf,"pse3");
		job.setJarByClass(PSE3Tool.class);
		job.setMapperClass(ParallelSuperEyeballMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(STripleOutputFormat.class);
		job.setOutputKeyClass(Node.class);
		job.setOutputValueClass(NodePair.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		MultipleOutputs.addNamedOutput(job, "rejected", TextOutputFormat.class, Text.class, Text.class);
		return job.waitForCompletion(true) ? 0 :1;
		
	}

}
