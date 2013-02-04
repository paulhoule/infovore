package com.ontology2.millipede.sink;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import com.ontology2.millipede.Codec;
import com.ontology2.millipede.FileOpener;

public class SingleFileSink<T> extends CodecSink {

	public SingleFileSink(Codec<T> codec, String filename) throws Exception {
		super(codec, new LineSink(new FileOpener().createWriter(filename)));
		// TODO Auto-generated constructor stub
	}


}
