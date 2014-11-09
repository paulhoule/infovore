package com.ontology2.rdf;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.ontology2.rdf.parser.NodeParser;
import com.ontology2.rdf.parser.ParseException;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JenaUtil {

    public static RDFNode fetchScalar(Dataset m,Query query) throws Exception {
        return fetchScalar(m,query,null);
    }

    public static RDFNode fetchScalar(Dataset m,Query query,QuerySolution bindings) throws Exception {
        QueryExecution qe=QueryExecutionFactory.create(query,m);

        try {
            if(null!=bindings) {
                qe.setInitialBinding(bindings);
            }

            ResultSet results=qe.execSelect();
            if(!results.hasNext())
                return null;

            List<String> vars=results.getResultVars();
            if(vars.size()!=1) {
                throw new Exception("Scalar query returns other than one column");
            }

            QuerySolution row=results.nextSolution();
            RDFNode value=row.get(vars.get(0));
            if (results.hasNext()) {
                throw new Exception("Scalar query returns more than one row");
            }
            return value;
        } finally { qe.close(); }
    }

    public static Map<RDFNode,RDFNode> fetchMap(Dataset m,Query query,QuerySolution bindings) throws Exception {
        QueryExecution qe=QueryExecutionFactory.create(query,m);		
        try {
            ResultSet results=qe.execSelect();
            Map<RDFNode,RDFNode> map=Maps.newHashMap();
            List<String> vars=results.getResultVars();

            while(results.hasNext()) {
                QuerySolution row=results.nextSolution();
                map.put(row.get(vars.get(0)),row.get(vars.get(1)));
            }
            return map;
        } finally { qe.close(); }
    }

    //
    // fetch as a map but reject results that occur more than once
    //

    public static Map<RDFNode,RDFNode> fetchMapSingle(Dataset m,Query query,QuerySolution bindings) throws Exception {
        QueryExecution qe=QueryExecutionFactory.create(query,m);		
        try {
            ResultSet results=qe.execSelect();
            Set<RDFNode> seen=Sets.newHashSet();
            Map<RDFNode,RDFNode> map=Maps.newHashMap();
            List<String> vars=results.getResultVars();

            while(results.hasNext()) {
                QuerySolution row=results.nextSolution();
                RDFNode key=row.get(vars.get(0));
                if(seen.contains(key)) {
                    map.remove(key);
                } else {
                    seen.add(key);
                    map.put(key,row.get(vars.get(1)));
                }
            }
            return map;
        } finally { qe.close(); }
    }

    //
    // note that we append to the first model specified and use the second model as a source
    // to run the query against
    //
    public static void appendConstruct(Model outModel,
            String queryString,
            Model inModel,
            QuerySolution bindings) {
        Query query=QueryFactory.create(queryString);
        QueryExecution qe=QueryExecutionFactory.create(query,inModel);
        try {
            if(null!=bindings) {
                qe.setInitialBinding(bindings);
            }

            qe.execConstruct(outModel);
        } finally{ qe.close(); }
    }

    public static void appendConstruct(Model outModel,String queryString,Model inModel) {
        appendConstruct(outModel,queryString,inModel,null);
    }

    public static void appendConstruct(Model theModel,String queryString) {
        appendConstruct(theModel,queryString,theModel,null);
    }

    public static LoadingCache<String,Node> createNodeParseCache() {
        return CacheBuilder.newBuilder().maximumSize(100000).build(
                new CacheLoader<String,Node> () {
                    public Node load(String that) throws Exception {
                        return ParseNode(that);
                    }
                });
    }

    public static Node ParseNode(String lexicalForm) throws ParseException {
        NodeParser parser=new NodeParser(new StringReader(lexicalForm));
        parser.parse();
        return parser.getNodeValue();
    };
}
