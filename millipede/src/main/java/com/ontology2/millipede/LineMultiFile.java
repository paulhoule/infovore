package com.ontology2.millipede;

import static java.lang.Math.*;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Formatter;
import java.util.Locale;

import com.google.common.io.OutputSupplier;
import com.ontology2.millipede.sink.CodecSink;
import com.ontology2.millipede.sink.LineSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.CodecSource;
import com.ontology2.millipede.source.LineSource;
import com.ontology2.millipede.source.Source;

public class LineMultiFile<T> extends MultiFile<T> implements PullMultiSource<T> {
    private final Codec codec;

    public LineMultiFile(String directory,String nameBase,String nameExtension,PartitionFunction<T> f,Codec codec) {
        super(directory,nameBase,nameExtension,f);
        this.codec=codec;
    };

    public Codec<T> getCodec() {
        return codec;
    }

    protected LineSink createLineSink(int binNumber) throws Exception {
        return new LineSink(createWriter(binNumber));
    }

    public long pushBin(int binNumber,Sink<T> destination) throws Exception {
        Source<T> source = createSource(binNumber);
        return Plumbing.flow(source, destination);
    }

    public Sink<T> createSink(int binNumber) throws Exception {
        return new CodecSink(codec,createLineSink(binNumber));
    }


    public Source<T> createSource(int binNumber) throws Exception {
        return new CodecSource<T>(codec,createLineSource(binNumber));
    }

    private LineSource createLineSource(int binNumber) throws Exception {
        return new LineSource(createReader(binNumber));
    }
}