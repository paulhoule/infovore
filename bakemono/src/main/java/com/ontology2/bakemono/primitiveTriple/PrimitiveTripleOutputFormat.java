package com.ontology2.bakemono.primitiveTriple;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.jena.TripleOutputFormat.TripleRecordWriter;
import com.ontology2.centipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.centipede.primitiveTriples.PrimitiveTripleCodec;

abstract public class PrimitiveTripleOutputFormat<K,V> extends FileOutputFormat<K, V> {

    public class TripleRecordWriter extends RecordWriter<K, V> {

        private final PrintWriter innerOutput;
        private final PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();

        public TripleRecordWriter(PrintWriter innerOutput) {
            this.innerOutput=innerOutput;
        }
        @Override
        public void write(K key, V value) throws IOException {
            innerOutput.println(ptc.encode(makeTriple(key,value)));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
            innerOutput.close();
        }

    }

    abstract protected PrimitiveTriple makeTriple(K key, V value);
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext ctx) throws IOException {
        return new TripleRecordWriter(createRawOutputStream(ctx));

    }

    private PrintWriter createRawOutputStream(
            TaskAttemptContext ctx) throws IOException {
        boolean isCompressed = getCompressOutput(ctx);

        if (!isCompressed) {
            Path file = getDefaultWorkFile(ctx, ".nt");
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            return new PrintWriter(fs.create(file, false));
        } else {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(ctx, GzipCodec.class);

            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
            Path file =  getDefaultWorkFile(ctx, ".nt"+codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(ctx.getConfiguration());
            FSDataOutputStream fileOut = fs.create(file, false);
            return new PrintWriter(codec.createOutputStream(fileOut));
        }
    }
}
