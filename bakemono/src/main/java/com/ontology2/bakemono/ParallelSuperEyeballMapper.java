package com.ontology2.bakemono;

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ParallelSuperEyeballMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
	private static org.apache.commons.logging.Log logger = LogFactory.getLog(FreebaseRDFMapper.class);
	
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
