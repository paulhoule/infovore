package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Triple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import java.io.DataOutputStream;
import java.io.IOException;


abstract public class TripleOutputFormat<K,V> extends FileOutputFormat<K, V> {

    public class TripleRecordWriter extends RecordWriter<K, V> {

        private final DataOutputStream innerOutput;
        private final StreamRDF innerSink;

        public TripleRecordWriter(DataOutputStream innerOutput) {
            this.innerOutput=innerOutput;
            this.innerSink = StreamRDFWriter.getWriterStream(innerOutput, Lang.NTRIPLES);
        }
        @Override
        public void write(K key, V value) throws IOException {
            this.innerSink.triple((makeTriple(key, value)));
        }



        @Override
        public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
            innerSink.finish();
            innerOutput.close();
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
            return fs.create(file, false);
        } else {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(ctx, GzipCodec.class);

            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            Path file =  getDefaultWorkFile(ctx, ".nt"+codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            FSDataOutputStream fileOut = fs.create(file, false);
            return new DataOutputStream(codec.createOutputStream(fileOut));
        }
    }
}
