package com.ontology2.hydroxide;

import com.google.common.base.Predicate;

public class SubjectIs implements Predicate<FreebaseQuad>{

	public final String subject;
	
	public SubjectIs(String subject) {
		this.subject=subject;
	}
	
	@Override
	public boolean apply(FreebaseQuad arg0) {
		return arg0.getSubject().equals(subject);
	}

}
