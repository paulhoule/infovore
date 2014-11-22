package com.ontology2.haruhi.ssh;

import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;

import static java.util.Objects.hash;

public class HadoopConfigurationVariable {
    public BootstrapActions.ConfigFile getConfigFile() {
        return configFile;
    }

    final BootstrapActions.ConfigFile configFile;

    public String getKey() {
        return key;
    }

    final String key;

    public HadoopConfigurationVariable(String key) {
        this(BootstrapActions.ConfigFile.Mapred,key);
    }
    public HadoopConfigurationVariable(BootstrapActions.ConfigFile configFile, String key) {
        this.configFile = configFile;
        this.key = key;
    }

    @Override
    public int hashCode() {
        return hash(configFile, key);
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof HadoopConfigurationVariable) {
            HadoopConfigurationVariable that=(HadoopConfigurationVariable) other;
            return this.configFile.equals(that.configFile)
                    && this.key.equals(that.key);
        }
        return false;
    }
}
