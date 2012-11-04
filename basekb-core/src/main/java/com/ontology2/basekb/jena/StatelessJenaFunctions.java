package com.ontology2.basekb.jena;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.basekb.BaseIRI;

import static com.ontology2.basekb.StatelessIdFunctions.*;

public class StatelessJenaFunctions {
	
	public static Node fbToUri(String mid) {
		return Node.createURI(BaseIRI.freebaseBase+mid.substring(1).replace("/", "."));
	}
	
	public static RDFNode fromFb(Model m,String fbId) {
		return m.asRDFNode(fromFb(fbId));
	}
	
	public static Node fromFb(String fbId) {
		if ("".equals(fbId))
			return Node.createURI(BaseIRI.freebaseBase);
		
		return Node.createURI(BaseIRI.freebaseBase + fbId.substring(1).replace('/', '.'));
	}
	
	public static RDFNode toBk(Model m,String fbId) {
		return m.asRDFNode(toBk(fbId));
	}
	
	public static Node toBk(String fbId) {
		return Node.createURI(toBkUri(fbId));
	}
	
	public static Node toBkKeyProperty(String fbId) {
		if ("".equals(fbId))
			return Node.createURI(BaseIRI.bkNs);
		
		return Node.createURI(BaseIRI.bkPublic + "hasKey." + fbId.substring(1).replace('/', '.'));			
	}
	
	public static RDFNode toBkKeyProperty(Model m,String fbId) {
		return m.asRDFNode(toBkKeyProperty(fbId));
	}
	
	public static RDFNode toBkProperty(Model m,String fbId) {
		return m.createProperty(BaseIRI.bkNs + fbId.substring(1).replace('/', '.'));
	}
	
	
	public static boolean isMid(RDFNode r) {
		if(!r.isResource())
			return false;
		
		String uri=((Resource) r).getURI();
		return uri.startsWith(BaseIRI.freebaseBase+"m.");
	}
	
	public static String toFb(Node fb) {
		String url=fb.toString();
		if(url.startsWith(BaseIRI.bkNs)) {
			return "/"+url.substring(BaseIRI.bkNs.length()).replace(".", "/");			
		}
		
		if(url.startsWith(BaseIRI.freebaseBase)) {
			return "/"+url.substring(BaseIRI.freebaseBase.length()).replace(".", "/");			
		}
		
		return null;
	}
	
	public static String toFb(RDFNode fb) {
		return toFb(fb.asNode());
	}

}
