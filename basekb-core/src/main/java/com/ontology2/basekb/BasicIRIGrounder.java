package com.ontology2.basekb;

import com.google.common.base.Splitter;
import static com.ontology2.basekb.StatelessIdFunctions.*;

public class BasicIRIGrounder implements IRIGrounder {
	public final NameResolutionStep step;
	
	public BasicIRIGrounder(NameResolutionStep step) {
		this.step=step;
	}

	@Override
	public String lookup(String name) {
		if (!name.startsWith(BaseIRI.bkNs))
			return name;
		
		if (name.startsWith(BaseIRI.bkNs+"m."))
			return name;
		
		if (name.startsWith(BaseIRI.guidPrefix)) {
			String guidPart="#"+name.substring(BaseIRI.guidPrefix.length());
			String fbMid=guidToMid(guidPart);
			return toBkUri(fbMid);
		}
		
		String localName=name.substring(BaseIRI.bkNs.length());		
		String mid=BaseIRI.rootNode;
		for(String part:Splitter.on(".").split(localName)) {
			if(null==mid) {
				if(!part.isEmpty()) {
					throw new RuntimeException("Syntax error in name <"+name+">");
				}
				
				mid=BaseIRI.rootNode;
			} else {
				mid=step.lookup(mid,part);
				if (null==mid)
					return name;
			}
		}

		return mid;
	}

}
