package com.ontology2.millipede.shell;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class MillipedeShell extends CommandLineApplication {
	private static Log logger = LogFactory.getLog(MillipedeShell.class);
	public MillipedeShell(String[] arguments) {
		super(arguments);
	}

	public static void main(String[] args) throws IOException {
		new MillipedeShell(args).run();
	}

	@Override
	protected void _run() throws Exception {
		SetMultimap<String, ClassInfo> appClasses = getAppClasses();
		if(arguments.length<2) {
			usage();
		}
		
		String action=arguments[0];
		String application=arguments[1];
		
		if(!action.equals("run")) {
			usage();
		}
		
		
		String appName=application+"App";
		Set<ClassInfo> classes=appClasses.get(appName);
		
		if(classes.size()==0)
			throw new Exception("Couldn't find any class with name ["+appName+"]");
		
		if (classes.size()>1) {
			throw new Exception("Found more than one class named ["+appName+"]");
		}
		
		String[] innerArguments=
				arguments.length<3 ? new String[0] : Arrays.copyOfRange(arguments, 2, arguments.length);
		Class<CommandLineApplication> clazz=(Class<CommandLineApplication>) classes.iterator().next().load();
		Constructor<CommandLineApplication> c=clazz.getConstructor(String[].class);
		CommandLineApplication app=c.newInstance((Object) innerArguments);
		app.run();
	}

	
	/**
	 * @return the name of the shell script that wraps this application
	 */
	public String getShellName() {
		return "millipede";
	}
	
	private void usage() {
		System.out.println("usage:");
		System.out.println();
		System.out.println(getShellName()+" <action> <application> ...");
		System.out.println();
		System.out.println("<action> = run");
		System.out.println("<application> the shell appends 'App' to this name and searches for a like");
		System.out.println("named class that implements CommandLineApplication");
		System.out.println();
		System.out.println("Additional parameters are passed to the application");
		System.exit(-1);
	}

	private SetMultimap<String, ClassInfo> getAppClasses() throws IOException {
		SetMultimap<String,ClassInfo> appClasses=HashMultimap.create();
		ClassPath cp=ClassPath.from(getClass().getClassLoader());
		for(ClassInfo i:cp.getTopLevelClasses()) {
			String name=i.getSimpleName();
			if(name.endsWith("App")) {
				try {
					Class thatClass=i.load();

					if (CommandLineApplication.class.isAssignableFrom(thatClass)) {
						appClasses.put(name, i);
					}
				} catch(Error ex) {
//					I get errors at i.load() above when this is running in the shell wrapper
//					but not in the Maven test runner;  currently it doesn't affect any of the classes
//					that are really meant to run with this so I'm eating this for now
					
					logger.debug("trouble loading class "+name,ex);
				}
			}
		}
		return appClasses;
	}

}
