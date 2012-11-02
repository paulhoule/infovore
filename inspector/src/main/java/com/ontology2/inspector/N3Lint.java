package com.ontology2.inspector;

import java.io.InputStream;
import java.io.Reader;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.*;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;
import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;
import com.ontology2.millipede.FileOpener;
import com.ontology2.tripleFilter.TripleCounter;
import com.ontology2.tripleFilter.TurtleParserWrapper;

public class N3Lint {

	public static void main(String[] args) throws Exception {
		System.out.println(args.length);
		if(0==args.length) {
			System.err.println("You must specify at least one file to test");
		}
		
		for(int i=0;i<args.length;i++) {
			String filename=args[i];
			TripleCounter counter=new TripleCounter();
			TurtleParserWrapper wrapper= new TurtleParserWrapper(filename,counter);
			wrapper.parse();
			System.out.println(filename+": "+counter.getCount()+" triples");
		}
	}

}
