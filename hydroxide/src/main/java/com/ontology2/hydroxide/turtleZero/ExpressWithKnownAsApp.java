package com.ontology2.hydroxide.turtleZero;

import java.util.Enumeration;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.OWL;
import com.ontology2.basekb.BaseIRI;
import com.ontology2.hydroxide.BKBPublic;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.sink.NTriplesSink;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

import jdbm.helper.Tuple;

public class ExpressWithKnownAsApp {
	static Logger logger=Logger.getLogger(ExpressWithKnownAsApp.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	static TurtleZero t0;
	static Sink<Triple> sink;
	static Set<String> blacklist=Sets.newHashSet();
	static int nameCount=0;
	
	public static void main(String[] args) throws Exception {
		t0=new TurtleZero();
//		blacklist.add(t0.lookup("/authority/wikipedia"));
//		blacklist.add(t0.lookup("/authority/musicbrainz"));
//		blacklist.add(t0.lookup("/wikipedia"));
//		blacklist.add(t0.lookup("/boot"));
		
		TripleMultiFile out=PartitionsAndFiles.getKnownAs();
		
		sink=new Partitioner(out);
		traverseNamespace("/m/01","",new Stack<String>());
		sink.close();
	}

	private static void traverseNamespace(String nsId,String prefix,Stack<String> ancestors) throws Exception {
		if (blacklist.contains(nsId))
			return;
		
		Enumeration<Tuple> list=t0.lookupNamespace(nsId);
		String masterPrefix=BaseIRI.bkNs.substring(0,BaseIRI.bkNs.length()-1);
		ancestors.push(nsId);
		while(list.hasMoreElements()) {
			Tuple t=list.nextElement();
			String key=(String) t.getKey();
			String value=(String) t.getValue();
			if (ancestors.contains(value)) {
				logger.warn("Circular reference involving namespace ["+nsId+"]");
				continue;
			}
			
			String[] keyParts=key.split("/");
			String localName=iriEscape(unescapeKey(keyParts[3]));
			String fullname=prefix+"/"+localName;
			if (CharMatcher.ASCII.matchesAllOf(fullname)) {
				sink.accept(new Triple(
					toBk(value),
					BKBPublic.knownAs,
					toBk(fullname)
				));
			}
			nameCount++;
			if (0==(nameCount % 10000)) {
				System.out.println(nameCount);
			}
			traverseNamespace(value,fullname,ancestors);
		}
		ancestors.pop();
	}

}
