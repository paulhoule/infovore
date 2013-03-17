package com.ontology2.hydroxide.shell;

import com.ontology2.millipede.shell.SpringCommandLineApplication;

public class ShellTestApp extends SpringCommandLineApplication {
	static boolean gotHit=false;
	static String[] lastArguments;
	
	public ShellTestApp() { 
		gotHit=false;
		lastArguments=null;
	}

	protected void _run(String[] arguments) throws Exception {
		System.out.println("Running shell test application");
		for(int i=0;i<arguments.length;i++) {
			System.out.println(i+": "+arguments[i]);
		}
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
