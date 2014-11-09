package com.ontology2.haruhi;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MavenManagedJarTests {
    private MavenManagedJar that;
    final String repoPath="/home/jimmy/.m2/repository";
    final String s3Path="s3://somewhere";
    
    @Before
    public void setup() {
        that=new MavenManagedJar();
        that.setArtifactId("unicorn");
        that.setGroupId("com.example");
        that.setVersion("8");
        that.setClassifier("paradoxical");
    }
    
    @Test
    public void checkwholeLocalPath() {
        String result=that.pathFromLocalMavenRepository(repoPath);
        assertEquals(
                repoPath+"/com/example/unicorn/8/unicorn-8-paradoxical.jar"
                ,result);
    }
    
    @Test
    public void checkwholeS3Path() {
        String result=that.s3JarLocation(s3Path);
        assertEquals(
                s3Path+"/unicorn-8-paradoxical.jar"
                ,result);
    }
    
    @Test
    public void s3WorksWhenPathEndsWithSlash() {
        String result=that.s3JarLocation(s3Path+"/");
        assertEquals(
                s3Path+"/unicorn-8-paradoxical.jar"
                ,result);
    }
}
