package com.ontology2.hydroxide.files;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.ontology2.hydroxide.InvalidNodeException;
import com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.sink.Sink;

public class ReadNTriples implements Sink<String> {
	final Sink<PrimitiveTriple> acceptSink;
	final Sink<String> rejectSink;
	private static Log logger = LogFactory.getLog(ReadNTriples.class);
	final static Pattern tripleRegex = Pattern.compile("\\s*(<[^>]*>)\\s+(<[^>]*>)\\s+(.*?)\\s*[.]");
	final static Pattern commentRegex = Pattern.compile("\\s*#.*");
	
	public ReadNTriples(Sink<PrimitiveTriple> acceptSink,Sink<String> rejectSink) {
		this.acceptSink = acceptSink;
		this.rejectSink = rejectSink;
	}

	@Override
	public void accept(String obj) throws Exception {
		if(obj.isEmpty()) {
			return;
		}
		
		try {
			String[] parts = splitTriple(obj);
			if(parts.length==3) {
				acceptSink.accept(new PrimitiveTriple(parts[0],parts[1],parts[2]));
			}
		} catch(InvalidNodeException ex) {
			logger.warn("Invalid triple: "+obj);
			rejectSink.accept(obj);
			return;					
		}
	}

	@Override
	public void close() throws Exception {
		acceptSink.close();
		rejectSink.close();
	}

	static String[] splitTriple(String obj) throws InvalidNodeException {
		Matcher m=tripleRegex.matcher(obj);
		if(!m.matches()) {
			Matcher m2=commentRegex.matcher(obj);
			if (!m2.matches()) {
				throw new InvalidNodeException();			
			}
			
			return new String[0];
		}

		return new String[] {m.group(1),m.group(2),m.group(3)};			

	}
}
