package com.ontology2.hydroxide;

import javax.annotation.Nullable;

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
			public boolean apply(@Nullable PrimitiveTriple arg0) {
				return arg0.predicate.equals(thatPredicate);
			}
		};
	}
	
	
}
