package com.ontology2.haruhi;

import static org.junit.Assert.*;

import java.net.MalformedURLException;

import org.junit.Before;
import org.junit.Test;

public class MavenManagedJarTests {
    private MavenManagedJar that;
    final String repoPath="/home/jimmy/.m2/repository";
    
    @Before
    public void setup() {
        that=new MavenManagedJar();
        that.setArtifactId("unicorn");
        that.setGroupId("com.example");
        that.setVersion("8");
        that.setClassifier("paradoxical");
    }
    
    @Test
    public void checkwholePath() {
        String result=that.pathFromLocalMavenRepository(repoPath);
        assertEquals(
                repoPath+"/com/example/unicorn/8/unicorn-8-paradoxical.jar"
                ,result);
    }
}
