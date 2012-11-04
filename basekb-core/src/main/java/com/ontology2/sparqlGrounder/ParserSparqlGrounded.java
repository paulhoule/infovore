package com.ontology2.sparqlGrounder;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.openjena.atlas.logging.Log;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.sparql.lang.Parser;
import com.hp.hpl.jena.sparql.lang.ParserFactory;
import com.hp.hpl.jena.sparql.lang.ParserRegistry;
import com.hp.hpl.jena.sparql.lang.ParserSPARQL11;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11TokenManager;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.ontology2.basekb.IRIGrounder;
import com.ontology2.basekb.jena.DefaultSpringConfiguration;

//
// The business end of this code overrides SPARQLParser11.createNode()
// but it takes about 140 loc to do that.
//
//

public class ParserSparqlGrounded extends Parser {
	private class SparqlParserGrounded extends SPARQLParser11 {

		public SparqlParserGrounded(Reader stream) {
			super(stream);
		}

		@Override
		protected Node createNode(String iri) {
			try {
				return Node.createURI(grounder.lookup(iri));
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

	}
	
	final IRIGrounder grounder;
	final Map<String, String> prefixMapping;
	
	public ParserSparqlGrounded(IRIGrounder grounder, Map<String, String> prefixMapping) {
		this.grounder=grounder;
		this.prefixMapping=prefixMapping;
	}

	public static void register(IRIGrounder grounder) {
		register(grounder,new HashMap<String,String>(), O2Syntax.syntaxGroundedSPARQL);
	}
	
	final static Set<Syntax> registeredList=Sets.newHashSet();
	
	public static void register(final IRIGrounder grounder,final Map<String, String> prefixMapping, final Syntax mySyntax) {
		
		//
		// since we undo the static scope in the classes that use this,  we're tougher
		// about registering Syntaxes twice than Jena is
		//
		
		if(registeredList.contains(mySyntax)) {
			throw new RuntimeException("You attempted to register syntax ["+mySyntax+"] more than once");
		}
		
		ParserRegistry.addFactory(mySyntax, new ParserFactory() {

			@Override
			public boolean accept(final Syntax syntax ) { return mySyntax.equals(syntax) ; } 

			@Override
			public Parser create(final Syntax syntax) {
				return new ParserSparqlGrounded(grounder,prefixMapping) ; 
			}
		});
	}
	
    private interface Action { void exec(SPARQLParser11 parser) throws Exception ; }
    
    @Override
    protected Query parse$(final Query query, String queryString)
    {
        query.setSyntax(O2Syntax.syntaxGroundedSPARQL);

        Action action = new Action() {
            @Override
            public void exec(SPARQLParser11 parser) throws Exception
            {
                parser.QueryUnit() ;
            }
        } ;

        appendNamespaces(query);
        perform(query, queryString, action) ;
        validateParsedQuery(query) ;
        return query ;
    }
    
    
    private void appendNamespaces(Query query) {
    	if (null==prefixMapping)
    		return;
    	
    	for(Entry<String, String> mapping:prefixMapping.entrySet()) {
    		String prefix=mapping.getKey();
    		String uri=mapping.getValue();
    		
    		if (null==query.getPrefix(prefix)) {
    			query.setPrefix(prefix, uri);
    		}
    	}
	}

	// All throwable handling.
    private void perform(Query query, String string, Action action)
    {
        Reader in = new StringReader(string) ;
        SPARQLParser11 parser = new SparqlParserGrounded(in);

        try {
            query.setStrict(true) ;
            parser.setQuery(query) ;
            action.exec(parser) ;
        }
        catch (com.hp.hpl.jena.sparql.lang.sparql_11.ParseException ex)
        { 
            throw new QueryParseException(ex.getMessage(),
                                          ex.currentToken.beginLine,
                                          ex.currentToken.beginColumn
                                          ) ; }
        catch (com.hp.hpl.jena.sparql.lang.sparql_11.TokenMgrError tErr)
        {
            // Last valid token : not the same as token error message - but this should not happen
            int col = parser.token.endColumn ;
            int line = parser.token.endLine ;
            throw new QueryParseException(tErr.getMessage(), line, col) ; }
        
        catch (QueryException ex) { throw ex ; }
        catch (JenaException ex)  { throw new QueryException(ex.getMessage(), ex) ; }
        catch (Error err)
        {
            // The token stream can throw errors.
            throw new QueryParseException(err.getMessage(), err, -1, -1) ;
        }
        catch (Throwable th)
        {
            Log.warn(ParserSparqlGrounded.class, "Unexpected throwable: ",th) ;
            throw new QueryException(th.getMessage(), th) ;
        }
    }
}
