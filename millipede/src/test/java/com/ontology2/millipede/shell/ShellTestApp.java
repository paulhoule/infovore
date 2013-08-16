package com.ontology2.millipede.shell;

import com.ontology2.millipede.shell.CommandLineApplication;

public class ShellTestApp extends CommandLineApplication {
    static boolean gotHit=false;
    static String[] lastArguments;

    public ShellTestApp() { 
        gotHit=false;
        lastArguments=null;
    }

    protected void _run(String[] arguments) throws Exception {
        System.out.println("Running shell test application");
        gotHit=true;
        lastArguments=arguments;
    }

    static void reset() {
        gotHit=false;
        lastArguments=null;
    }

    static boolean getGotHit() {
        return gotHit;
    }

    public static String[] getLastArguments() {
        return lastArguments;
    }

}
