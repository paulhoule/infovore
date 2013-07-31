package com.ontology2.bakemono.tools.freebasePrefilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
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

import com.ontology2.bakemono.Main;
import com.ontology2.bakemono.mappers.freebasePrefilter.FreebaseRDFMapper;

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
		if(arg0.length!=2)
			Main.errorCausedByUser("You must specify both input and output paths");
		
		String input=arg0[0];
		String output=arg0[1];
		
		JobConf conf = new JobConf(configuration,FreebaseRDFTool.class);
		conf.setJobName("prefilter");  
		conf.setOutputKeyClass(Text.class);  
		conf.setOutputValueClass(Text.class);  
		conf.setMapperClass(FreebaseRDFMapper.class);
		conf.setNumReduceTasks(50);
		conf.setPartitionerClass(HashPartitioner.class);
		conf.setInputFormat(TextInputFormat.class);  
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		FileInputFormat.addInputPath(conf,new Path(input));
		FileOutputFormat.setOutputPath(conf,new Path(output));
		FileOutputFormat.setCompressOutput(conf, true);
		FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		RunningJob job=JobClient.runJob(conf);
		job.waitForCompletion();
		return job.getJobState();
	}

}
