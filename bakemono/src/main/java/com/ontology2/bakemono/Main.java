package com.ontology2.bakemono;

import com.google.common.collect.Lists;

import java.util.List;

public class Main extends MainBase {

    public Main(String[] arg0) {
        super(arg0);
    }

    public List<String> getApplicationContextPath() {
        return Lists.newArrayList("com/ontology2/bakemono/applicationContext.xml");
    }

    public static void main(String[] arg0) throws Exception {
        new Main(arg0).run();
    }

}
