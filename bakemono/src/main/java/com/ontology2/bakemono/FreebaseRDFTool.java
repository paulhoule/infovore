package com.ontology2.bakemono;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;

public class FreebaseRDFTool implements Tool {

	private Configuration configuration=new Configuration();

	@Override
	public Configuration getConf() {
		return this.configuration;
	}

	@Override
	public void setConf(Configuration conf) {
		this.configuration=conf;

	}

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = new JobConf(configuration,FreebaseRDFTool.class);
		conf.setJobName("prefilter");  
		conf.setOutputKeyClass(Text.class);  
		conf.setOutputValueClass(Text.class);  
		conf.setMapperClass(FreebaseRDFMapper.class);
		conf.setNumReduceTasks((int) Math.round(conf.getNumMapTasks()*1.75));
		conf.setPartitionerClass(HashPartitioner.class);
		conf.setInputFormat(TextInputFormat.class);  
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		FileInputFormat.addInputPath(conf,new Path("/5percent.bz2"));
		FileOutputFormat.setOutputPath(conf,new Path("/ntriples.gz"));
		FileOutputFormat.setCompressOutput(conf, true);
		FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		RunningJob job=JobClient.runJob(conf);
		job.waitForCompletion();
		return job.getJobState();
	}

}
