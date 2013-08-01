package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Node;

public class NodePair {
    final Node one;
    final Node two;

    public NodePair(Node one,Node two) {
        this.one=one;
        this.two=two;
    };

    public Node getOne() {
        return one;
    }

    public Node getTwo() {
        return two;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((one == null) ? 0 : one.hashCode());
        result = prime * result + ((two == null) ? 0 : two.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NodePair other = (NodePair) obj;
        if (one == null) {
            if (other.one != null)
                return false;
        } else if (!one.equals(other.one))
            return false;
        if (two == null) {
            if (other.two != null)
                return false;
        } else if (!two.equals(other.two))
            return false;
        return true;
    }
}
