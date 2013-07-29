package com.ontology2.bakemono.jena;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;

public class STripleOutputFormat extends FileOutputFormat<Node_URI, NodePair> {

	public class TripleRecordWriter implements RecordWriter<Node_URI, NodePair> {

		private final DataOutputStream innerOutput;
		private final SinkTripleOutput innerSink;

		public TripleRecordWriter(DataOutputStream innerOutput) {
			this.innerOutput=innerOutput;
			this.innerSink = new SinkTripleOutput(innerOutput);
		}
		@Override
		public void write(Node_URI key, NodePair value) throws IOException {
			this.innerSink.send((makeTriple(key,value)));
		}

		private Triple makeTriple(Node_URI key, NodePair value) {
			return new Triple(key,value.getOne(),value.getTwo());
		}
		
		@Override
		public void close(Reporter reporter) throws IOException {
			innerSink.close();
		}

	}

	@Override
	public RecordWriter<Node_URI, NodePair> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		boolean isCompressed = getCompressOutput(job);
		   
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new TripleRecordWriter(fileOut);
		} else {
			Class<? extends CompressionCodec> codecClass =
					getOutputCompressorClass(job, GzipCodec.class);
	
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
			Path file =  FileOutputFormat.getTaskOutputPath(job, 
	                    name + codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new TripleRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
		}

	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		super.checkOutputSpecs(ignored,job);
	}

}
