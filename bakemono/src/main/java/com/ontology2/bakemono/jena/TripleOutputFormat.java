package com.ontology2.bakemono.jena;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;


abstract public class TripleOutputFormat<K,V> extends FileOutputFormat<K, V> {
    private DataOutputStream innerStream;

    public class TripleRecordWriter extends RecordWriter<K, V> {

        private final DataOutputStream innerOutput;
        private final SinkTripleOutput innerSink;

        public TripleRecordWriter(DataOutputStream innerOutput) {
            this.innerOutput=innerOutput;
            this.innerSink = new SinkTripleOutput(innerOutput);
        }
        @Override
        public void write(K key, V value) throws IOException {
            this.innerSink.send((makeTriple(key,value)));
        }



        @Override
        public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
            innerSink.close();
            innerStream.close();
        }

    }

    abstract protected Triple makeTriple(K key, V value);
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext ctx) throws IOException {
        return new TripleRecordWriter(createRawOutputStream(ctx));

    }

    private DataOutputStream createRawOutputStream(
            TaskAttemptContext ctx) throws IOException {
        boolean isCompressed = getCompressOutput(ctx);

        if (!isCompressed) {
            Path file = getDefaultWorkFile(ctx, ".nt");
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            FSDataOutputStream innerStream = fs.create(file, false);
            return innerStream;
        } else {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(ctx, GzipCodec.class);

            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            Path file =  getDefaultWorkFile(ctx, ".nt"+codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            FSDataOutputStream fileOut = fs.create(file, false);
            innerStream = new DataOutputStream(codec.createOutputStream(fileOut));
            return innerStream;
        }
    }
}
