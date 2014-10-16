package com.ontology2.haruhi;

import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.InputStreamResource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class ApplicationConfigurationFetcher {
    private final ApplicationContext applicationContext;

    File localPath=new File(System.getProperty("user.home"),".m2/repository/com/ontology2/bakemono/2.4-SNAPSHOT/bakemono-2.4-SNAPSHOT-metadata.jar");

    public ApplicationConfigurationFetcher(ApplicationContext applicationContext) {
        this.applicationContext=applicationContext;
    }

    public boolean testJarExists() {
        return localPath.exists();
    }

    public InputStream getContextXml() throws IOException {
        ZipFile zipFile = new ZipFile(localPath);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry=entries.nextElement();
            if(entry.getName().endsWith("/metadataContext.xml"))
                return zipFile.getInputStream(entry);
        }
        return null;
    }

    public ApplicationContext enrichedContext() throws IOException {
        GenericApplicationContext that=new GenericApplicationContext(applicationContext);
        XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(that);
        reader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD);
        reader.loadBeanDefinitions(new InputStreamResource(getContextXml()));
        that.refresh();
        return that;
    }


}
