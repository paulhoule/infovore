package com.ontology2.bakemono.primitiveTriples;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

abstract public class PrimitiveTripleOutputFormat<K,V> extends FileOutputFormat<K, V> {

    public class TripleRecordWriter extends RecordWriter<K, V> {

        private final OutputStream innerOutput;
        private final PrintWriter innerWriter;
        private final PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();

        public TripleRecordWriter(OutputStream innerOutput) {
            this.innerOutput=innerOutput;
            this.innerWriter=new PrintWriter(innerOutput);
        }
        @Override
        public void write(K key, V value) throws IOException {
            innerWriter.println(ptc.encode(makeTriple(key,value)));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
            innerWriter.close();
            innerOutput.close();
        }

    }

    abstract protected PrimitiveTriple makeTriple(K key, V value);
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext ctx) throws IOException {
        return new TripleRecordWriter(createRawOutputStream(ctx));

    }

    private OutputStream createRawOutputStream(
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
            return codec.createOutputStream(fileOut);
        }
    }
}
