package com.ontology2.millipede;

import java.io.InputStream;
import java.io.Reader;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangRIOT;

import com.hp.hpl.jena.sparql.core.Quad;
import com.ontology2.millipede.sink.Sink;

public class NQuadsMultiFile extends MultiFile<Quad> {

	public NQuadsMultiFile(String directory, String nameBase,
			String nameExtension, PartitionFunction<Quad> f) {
		super(directory, nameBase, nameExtension, f);
	}

	@Override
	public void pushBin(int binNumber, final Sink<Quad> destination) throws Exception {
		InputStream r=createInputStream(binNumber);
		try {
			LangRIOT parser=RiotReader.createParserNQuads(r, new org.openjena.atlas.lib.Sink<Quad>() {

				@Override
				public void close() {
				}

				@Override
				public void send(Quad item) {
					try {
						destination.accept(item);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void flush() {
				}
				
			});
			
			parser.parse();
			destination.close();
		} finally {
			r.close();
		}
		
	}

	@Override
	public Sink<Quad> createSink(int binNumber) throws Exception {
		throw new Exception("This method is not implemented yet!");
	}
	
	// Return raw strings
	
	public LineMultiFile<String> getLines() {
		return new LineMultiFile<String>(
				directory,
				nameBase,
				nameExtension,
				new DummyPartitionFunction(getPartitionCount()),
				new IdentityCodec());
	}
}
