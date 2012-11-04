package com.ontology2.basekb.jena;

import com.ontology2.basekb.BasicIRIGrounder;
import com.ontology2.basekb.CompositeIRIGrounder;

public class JenaIRIGrounder extends CompositeIRIGrounder {
	public JenaIRIGrounder(RawConfiguration config) {
		super(
			new BasicIRIGrounder(new JenaNameStep(config)),
			new JenaReplacedByFollower(config)
		);
	}
}
