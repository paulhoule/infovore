package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.ComparisonChain.start;

//
// Some apologies here.  The WritableNode does not support Blank nodes or Anonymous
// Nodes since these are things unlikely to show up in a triple file.
//

public class WritableNode implements WritableComparable {
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
            writeBigUTF8(out,innerNode.getURI());
            return;
        }
        
        String dt=innerNode.getLiteralDatatypeURI();
        if(dt!=null) {
            out.writeByte(GENERAL_LITERAL);
            writeBigUTF8(out,innerNode.getLiteralLexicalForm());
            writeBigUTF8(out,dt);
            return;
        }
        
        out.writeByte(STRING);
        writeBigUTF8(out,innerNode.getLiteralLexicalForm());
        writeBigUTF8(out,innerNode.getLiteralLanguage());
        return;
    }

    int nodeType() {
        return innerNode.isURI() ? URI
                : innerNode.getLiteralDatatypeURI()!=null ? GENERAL_LITERAL
                : STRING;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int type=in.readByte();
        if(type==URI) {
            String uri=readBigUTF8(in);
            innerNode=Node.createURI(uri);
        } else if (type==GENERAL_LITERAL) {
            String lexicalForm=readBigUTF8(in);
            String dt=readBigUTF8(in);
            innerNode=Node.createLiteral(lexicalForm,typeLookup.getSafeTypeByName(dt));
        } else if (type==STRING) {
            String lexicalForm=readBigUTF8(in);
            String language=readBigUTF8(in);
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

    //
    // this comparsion function may not conform with any particular standard
    //

    @Override
    public int compareTo(Object o) {
        if(o==null && !(o instanceof WritableNode))
            return -1;

        WritableNode that=(WritableNode) o;
        if (that.nodeType()>this.nodeType()) return -1;
        if (that.nodeType()<this.nodeType()) return 1;

        switch(this.nodeType()) {
            case URI: return innerNode.getURI().compareTo(that.innerNode.getURI());
            case GENERAL_LITERAL: return start()
                    .compare(this.getNode().getLiteralLexicalForm(), that.getNode().getLiteralLexicalForm())
                    .compare(this.getNode().getLiteralDatatypeURI(), that.getNode().getLiteralDatatypeURI())
                    .result();
            default: return start()
                    .compare(this.getNode().getLiteralLexicalForm(), that.getNode().getLiteralLexicalForm())
                    .compare(this.getNode().getLiteralLanguage(), that.getNode().getLiteralLanguage())
                    .result();
        }
    }

    public static void writeBigUTF8(DataOutput out,String that) throws IOException {
        byte[] utf8=that.getBytes(UTF_8);
        out.writeInt(utf8.length);
        out.write(utf8);
    }

    public static String readBigUTF8(DataInput in) throws IOException {
        byte[] utf8=new byte[in.readInt()];
        in.readFully(utf8);
        return new String(utf8,UTF_8);
    }
}
