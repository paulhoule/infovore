package com.ontology2.bakemono.jena;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;

public class STripleOutputFormat extends FileOutputFormat<Node_URI, NodePair> {

	private DataOutputStream innerStream;

	public class TripleRecordWriter extends RecordWriter<Node_URI, NodePair> {

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
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			innerSink.close();
			innerStream.close();
		}

	}

	@Override
	public RecordWriter<Node_URI, NodePair> getRecordWriter(TaskAttemptContext ctx) throws IOException {
		boolean isCompressed = getCompressOutput(ctx);

		if (!isCompressed) {
		    Path file = getDefaultWorkFile(ctx, ".nt");
		    FileSystem fs = file.getFileSystem(ctx.getConfiguration());
			FSDataOutputStream fileOut = fs.create(file, false);
			return new TripleRecordWriter(fileOut);
		} else {
			Class<? extends CompressionCodec> codecClass =
					getOutputCompressorClass(ctx, GzipCodec.class);
	
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
			Path file =  getDefaultWorkFile(ctx, ".nt"+codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(ctx.getConfiguration());
			FSDataOutputStream fileOut = fs.create(file, false);
			innerStream = new DataOutputStream(codec.createOutputStream(fileOut));
			return new TripleRecordWriter(innerStream);
		}

	}



}
