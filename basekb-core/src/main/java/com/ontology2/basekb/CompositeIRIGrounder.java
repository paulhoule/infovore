package com.ontology2.basekb;

public class CompositeIRIGrounder implements IRIGrounder {

	public final IRIGrounder[] grounders;
	public CompositeIRIGrounder(IRIGrounder... grounders) {
		this.grounders=grounders;
	}

	@Override
	public String lookup(String name) {
		for(IRIGrounder g:grounders) {
			name=g.lookup(name);
		};
		
		return name;
	}
	

}
