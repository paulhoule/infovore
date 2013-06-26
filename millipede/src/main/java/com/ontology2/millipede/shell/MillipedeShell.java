package com.ontology2.millipede.shell;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class MillipedeShell extends CommandLineApplication {
	
	private static Log logger = LogFactory.getLog(MillipedeShell.class);
	public MillipedeShell() {
		context=new ClassPathXmlApplicationContext(getApplicationContextPath().toArray(new String[] {}));
	}

	public List<String> getApplicationContextPath() {
		return Lists.newArrayList("com/ontology2/millipede/shell/applicationContext.xml");
	}
	
	private ApplicationContext context;
	@Override
	protected void _run(String[] arguments) throws Exception {

		if(arguments.length==0) {
			usage();
		}
		
		String action=arguments[0];
		if(action.equals("run")) {
			runAction(arguments);
		} else if(action.equals("list")) {
			listAction(arguments);			
		}
		

	}

	private void listAction(String[] arguments) {
		Map<String, CommandLineApplication> all = context.getBeansOfType(CommandLineApplication.class);
		for(Entry<String, CommandLineApplication> that:all.entrySet()) {
			String beanName=that.getKey();
			if(beanName.endsWith("App")) {
				String appName=beanName.substring(0, beanName.length()-3);
				System.out.println(appName);
			}
		}
	}

	public void runAction(String[] arguments) {
		String application=arguments[1];
			
		String appName=application+"App";
		CommandLineApplication app=null;
		try {
			app = context.getBean(appName,CommandLineApplication.class);
		} catch(BeanNotOfRequiredTypeException | NoSuchBeanDefinitionException ex) {
			die("Application ["+application+"] not found");
		};
		
		String[] innerArguments=
				arguments.length<3 ? new String[0] : Arrays.copyOfRange(arguments, 2, arguments.length);
		app.run(innerArguments);
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
		System.out.println(getShellName()+" list");
		System.out.println(getShellName()+" run <application> ...");
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
