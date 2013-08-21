package com.ontology2.haruhi;

import java.util.List;

import com.google.common.collect.Lists;
import com.ontology2.centipede.shell.CentipedeShell;

public class HaruhiShell extends CentipedeShell {

    @Override
    public List<String> getApplicationContextPath() {
        // TODO Auto-generated method stub
        List<String> that=Lists.newArrayList("com/ontology2/haruhi/shell/applicationContext.xml");
        return that;
    }

    @Override
    public String getShellName() {
        return "haruhi";
    }
    
    public static void main(String[] args) {
        new HaruhiShell().run(args);
    }
    
}
