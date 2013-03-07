package com.ontology2.millipede.primitiveTriples;

import com.google.common.base.Predicate;

public class PrimitiveTriple {
	public final String subject;
	public final String predicate;
	public final String object;
	
	public PrimitiveTriple(String subject, String predicate, String object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public static Predicate<PrimitiveTriple> hasPredicate(final String thatPredicate) {
		return new Predicate<PrimitiveTriple>() {
			@Override
			public boolean apply(PrimitiveTriple arg0) {
				return arg0.predicate.equals(thatPredicate);
			}
		};
	}
	
	public static Predicate<PrimitiveTriple> objectMatchesPrefix(final String thatPrefix) {
		return new Predicate<PrimitiveTriple>() {
			@Override
			public boolean apply(PrimitiveTriple arg0) {
				return arg0.object.startsWith(thatPrefix);
			}
		};
	}
	
	public String toString() {
		StringBuffer buff=new StringBuffer();
		buff.append(this.subject);
		buff.append("\t");
		buff.append(this.predicate);
		buff.append("\t");
		buff.append(this.object);
		buff.append(" .\n");
		return buff.toString();
	}
	
}
