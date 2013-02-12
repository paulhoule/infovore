package com.ontology2.hydroxide;

import java.util.Iterator;

import com.google.common.base.Splitter;
import com.ontology2.millipede.Codec;

public class PrimitiveTripleCodec implements Codec<PrimitiveTriple> {
	final Splitter tripleSplitter=Splitter.on("\t");

	@Override
	public String encode(PrimitiveTriple obj) {
		StringBuilder output=new StringBuilder();
		output.append(obj.subject);
		output.append("\t");
		output.append(obj.predicate);
		output.append("\t");
		output.append(obj.object);
		output.append(".");
		return output.toString();
	}

	@Override
	public PrimitiveTriple decode(String obj) {
		Iterator<String> parts=tripleSplitter.split(obj).iterator();
		String subject = parts.next();
		String predicate = parts.next();
		String object = parts.next();
		object=object.substring(0,object.length()-1);
		return new PrimitiveTriple(subject,predicate,object);
	}

}
