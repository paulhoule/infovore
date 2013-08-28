package com.ontology2.haruhi;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class MavenManagedJar {
    private String groupId;
    private String artifactId;
    private String version;
    private String classifier;
    
    private List<String> headArguments=Lists.newArrayList();
    
    public String getGroupId() {
        return groupId;
    }
    
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }
    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }
    public String getVersion() {
        return version;
    }
    public void setVersion(String versionId) {
        this.version = versionId;
    }
    public String getClassifier() {
        return classifier;
    }
    
    public void setClassifier(String classifierId) {
        this.classifier = classifierId;
    }
    
    public String pathFromLocalMavenRepository(String repository) {
        StringBuilder out=new StringBuilder();
        out.append(repository); out.append('/');
        
        for(String seg:Splitter.on(".").split(groupId)) {
            out.append(seg); out.append('/');
        }
        
        out.append(artifactId); out.append('/');
        out.append(version); out.append('/');
        out.append(artifactId); out.append('-');
        out.append(version);
        
        if(!classifier.isEmpty()) {
            out.append('-'); out.append(classifier);
        }
        
        out.append(".jar");
        return out.toString();
    };
    
    public List<String> getHeadArguments() {
        return headArguments;
    }

    public void setHeadArguments(List<String> headArguments) {
        this.headArguments = headArguments;
    }
}
