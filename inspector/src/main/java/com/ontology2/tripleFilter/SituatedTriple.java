package com.ontology2.tripleFilter;

import java.io.Reader;

import com.hp.hpl.jena.graph.Triple;

public class SituatedTriple {
	public final String file;
	public final int lineNumber;
	public final int columnNumber;
	public final Triple triple;
	
	public SituatedTriple(String file,int lineNumber,int columnNumber,Triple triple) {
		this.file=file;
		this.lineNumber=lineNumber;
		this.columnNumber=columnNumber;
		this.triple=triple;
	}
}
