package com.ontology2.haruhi;

import com.google.common.collect.Lists;
import com.ontology2.centipede.shell.CentipedeShell;

import java.io.File;
import java.util.List;

public class HaruhiShell extends CentipedeShell {

    @Override
    public List<String> getApplicationContextPath() {
        String $HOME=System.getProperty("user.home");
        
        List<String> that=Lists.newArrayList(
                "com/ontology2/centipede/shell/applicationContext.xml",
                "com/ontology2/haruhi/shell/applicationContext.xml"
        );
        
        File userConfigFile=new File($HOME,".haruhi/applicationContext.xml");
        if(userConfigFile.exists()) {
            that.add("file:"+userConfigFile.getAbsolutePath());
        }
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
