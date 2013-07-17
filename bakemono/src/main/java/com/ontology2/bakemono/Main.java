package com.ontology2.bakemono;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.Tool;

import com.hp.hpl.jena.graph.query.regexptrees.Text;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

public class Main implements Tool {
	private Configuration configuration;

	// Right now we are doing the Freebase prefilter in this spot to have
	// something specfic to accrete tests around.
	
	public Configuration getConf() {
		return this.configuration;
	}

	public void setConf(Configuration arg0) {
		this.configuration=arg0;
	}

	public int run(String[] arg0) throws Exception {
		JobConf conf = new JobConf(getConf(), Main.class);
		conf.setJobName("prefilter");  
		conf.setOutputKeyClass(Text.class);  
		conf.setOutputValueClass(PrimitiveTriple.class);  
		conf.setMapperClass(FreebaseRDFMapper.class);
		conf.setInputFormat(TextInputFormat.class);  
		conf.setOutputFormat(TextOutputFormat.class);  
		FileInputFormat.addInputPath(conf,new Path("/5percent.bz2"));
		FileOutputFormat.setOutputPath(conf,new Path("/ntriples.bz2"));
		RunningJob job=JobClient.runJob(conf);
		job.waitForCompletion();
		return job.getJobState();
	}

}
