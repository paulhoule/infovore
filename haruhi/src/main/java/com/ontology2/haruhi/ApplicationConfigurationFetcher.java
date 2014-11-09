package com.ontology2.haruhi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class ApplicationConfigurationFetcher {
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private Resource metadataJarPath;

    public boolean testJarExists() {
        return metadataJarPath.exists();
    }

    public InputStream getContextXml() throws IOException {
        ZipInputStream zipFile = new ZipInputStream(metadataJarPath.getInputStream());
        ZipEntry entry=zipFile.getNextEntry();
        while(entry!=null) {
            if(entry.getName().endsWith("/metadataContext.xml"))
                return zipFile;

            zipFile.closeEntry();
            entry=zipFile.getNextEntry();
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
