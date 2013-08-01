package com.ontology2.rdf.parser;

import static org.junit.Assert.*;

import java.io.StringReader;

import org.junit.Test;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.vocabulary.XSD;
import com.ontology2.rdf.parser.TokenMgrError;

public class NodeParserTest {

    @Test
    public void testXRI() throws ParseException {
        NodeParser n=new NodeParser(new StringReader("<http://ookaboo.com/>"));
        n.parse();
        Node node=n.getNodeValue();
        assert(null!=node);
        assert(node instanceof Node_URI);
        assertEquals("http://ookaboo.com/",node.getURI());
    }

    @Test
    public void testInt() throws ParseException {
        NodeParser n=new NodeParser(new StringReader("55"));
        n.parse();
        Node node=n.getNodeValue();
        assert(null!=node);
        assert(node.isLiteral());
        assertEquals(XSDDatatype.XSDinteger,node.getLiteralDatatype());
        assertEquals(55,node.getLiteralValue());
    }

    @Test(expected=TokenMgrError.class)
    public void testCrash() throws ParseException {
        NodeParser n=new NodeParser(new StringReader("<not http://a.uri/ by a>>ny means>"));
        n.parse();
    }
}
