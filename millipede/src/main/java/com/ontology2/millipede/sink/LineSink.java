package com.ontology2.millipede.sink;

import java.io.PrintWriter;


public class LineSink implements Sink<String> {

	private PrintWriter writer;

	public LineSink(PrintWriter writer) {
		this.writer=writer;
	}
	
	@Override
	public void accept(String obj) throws Exception {
		writer.println(obj);
		
	}

	@Override
	public void close() throws Exception {
		writer.close();
	}
	
}
