package com.ontology2.haruhi.flows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Assignment {
    private final String assignTo;
    private final String expression;

    public Assignment(String assignTo,String expression) {
        this.assignTo=assignTo;
        this.expression=expression;
    }

    public String getAssignTo() {
        return assignTo;
    }

    public String getExpression() {
        return expression;
    }
}
