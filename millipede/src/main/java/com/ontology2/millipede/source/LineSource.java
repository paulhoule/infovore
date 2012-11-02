package com.ontology2.millipede.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;


public class LineSource implements Source<String> {

	private BufferedReader reader;
	String nextLine;

	public LineSource(BufferedReader reader) throws IOException {
		this.reader = reader;
		nextLine=reader.readLine();
	}

	@Override
	public boolean hasMoreElements() {
		return nextLine != null;
	}

	@Override
	public String nextElement() throws Exception {
		String result=nextLine;
		nextLine=reader.readLine();
		return result;
	}

}
