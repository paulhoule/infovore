package com.ontology2.basekb.jena;

import java.net.URL;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.reflections.*;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ConfigurationBuilder;

import com.google.common.base.Joiner;
import com.ontology2.basekb.IRIGrounder;

public class DefaultSpringConfiguration implements GroundedSparqlConfiguration {
	
	//
	// Make basekb-tools and basekb-tests configurable via Spring 
	//
	
	private final ApplicationContext context;

	private DefaultSpringConfiguration() {
		String[] checkPaths=new String[] {"basekb-local.xml","basekb-defaults.xml"};
		String validPath=null;
		
		for(String path:checkPaths) {
			if(null!=getClass().getClassLoader().getResource(path)) {
				validPath=path;
				break;
			}
		}
		
		if (null==validPath)
			throw new RuntimeException("Could not find any configuration files in resources tree: "+Joiner.on(",").join(checkPaths));
		
		context= new ClassPathXmlApplicationContext(new String[] {validPath});
	}
	
	/* (non-Javadoc)
	 * @see com.ontology2.basekb.GroundedSparqlConfiguration#getJenaConfiguration()
	 */
	@Override
	public RawConfiguration getRawConfiguration() {		
		return context.getBean("jenaSparqlProvider",RawConfiguration.class);
	}

	/* (non-Javadoc)
	 * @see com.ontology2.basekb.GroundedSparqlConfiguration#getJenaIRIGrounder()
	 */
	@Override
	public IRIGrounder getIRIGrounder() {
		return context.getBean("jenaIRIGrounder",IRIGrounder.class);
	}
	
	/* (non-Javadoc)
	 * @see com.ontology2.basekb.GroundedSparqlConfiguration#getGroundedQueryFactory()
	 */
	@Override
	public AnyQueryFactory getGroundedQueryFactory() {
		return context.getBean("jenaGroundedQueryFactory",AnyQueryFactory.class);
	}
	
	private static class SingletonHolder {
		public static final GroundedSparqlConfiguration instance=new DefaultSpringConfiguration();
	}
	
	public static GroundedSparqlConfiguration getInstance() {
		return SingletonHolder.instance;
	}




}
