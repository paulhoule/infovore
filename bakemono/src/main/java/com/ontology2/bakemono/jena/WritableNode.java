package com.ontology2.bakemono.jena;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.jasper.tagplugins.jstl.core.If;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeFactory;

//
// Some apologies here.  The WritableNode does not support Blank nodes or Anonymous
// Nodes since these are things unlikely to show up in a triple file.
//

public class WritableNode implements Writable {
    Node innerNode;
    final static int URI=0;
    final static int STRING=1;
    final static int GENERAL_LITERAL=2;
    private TypeMapper typeLookup=TypeMapper.getInstance();
    
    // try to write this when this is null and you're in trouble,  this is only
    // intended for use if you wish to read
    
    public WritableNode() {
        innerNode=null;
    }
    
    public WritableNode(Node n) {
        innerNode=n;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if(innerNode.isURI()) {
            out.writeByte(URI);
            out.writeUTF(innerNode.getURI());
            return;
        }
        
        String dt=innerNode.getLiteralDatatypeURI();
        if(dt!=null) {
            out.writeByte(GENERAL_LITERAL);
            out.writeUTF(innerNode.getLiteralLexicalForm());
            out.writeUTF(dt);
            return;
        }
        
        out.writeByte(STRING);
        out.writeUTF(innerNode.getLiteralLexicalForm());
        out.writeUTF(innerNode.getLiteralLanguage());
        return;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int type=in.readByte();
        if(type==URI) {
            String uri=in.readUTF();
            innerNode=Node.createURI(uri);
        } else if (type==GENERAL_LITERAL) {
            String lexicalForm=in.readUTF();
            String dt=in.readUTF();
            innerNode=Node.createLiteral(lexicalForm,typeLookup.getSafeTypeByName(dt));
        } else if (type==STRING) {
            String lexicalForm=in.readUTF();
            String language=in.readUTF();
            innerNode=Node.createLiteral(lexicalForm,language,false);
        }
    }
    

    public Node getNode() {
        return innerNode;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof WritableNode)
                ? ((WritableNode) obj).getNode().equals(getNode())
                : false;
    }

    @Override
    public int hashCode() {
        return getNode().hashCode();
    }

}
