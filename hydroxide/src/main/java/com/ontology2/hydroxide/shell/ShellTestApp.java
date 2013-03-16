package com.ontology2.hydroxide.shell;

import com.ontology2.millipede.shell.CommandLineApplication;

public class ShellTestApp extends CommandLineApplication {
	static boolean gotHit=false;
	static String[] lastArguments;
	
	public ShellTestApp(String[] arguments) {
		super(arguments);
		gotHit=false;
		lastArguments=arguments;
	}

	protected void _run() throws Exception {
		System.out.println("Running shell test application");
		for(int i=0;i<getArguments().length;i++) {
			System.out.println(i+": "+getArguments()[i]);
		}
		gotHit=true;
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
