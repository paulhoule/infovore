package com.ontology2.centipede.shell;

//
// I run this any place where a Perl or PHP programmer might want to call die
// but this gives some control of what that means to the caller
//
public class ShutTheProcessDown extends RuntimeException {

    public ShutTheProcessDown() {
        super("process shutdown requested");
    }
    
}
