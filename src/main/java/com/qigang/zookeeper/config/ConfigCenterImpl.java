package com.qigang.zookeeper.config;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigCenterImpl implements ConfigCenter {
    private final static Logger logger = LoggerFactory.getLogger(ConfigCenter.class);
    private volatile Map<String, String> configProperties = new HashMap<String, String>();
    private ZkClient client;//
    private ConfigCenterWatcher configCenterWatcher;

    public ConfigCenterImpl(String connectionString){
        this.client=new ZkClient(connectionString);
        configCenterWatcher =new ConfigCenterWatcher(this.client,this);
        this.init();
    }

    @Override
    public void init() {
        if(!client.exists(configRoot)){
            client.createPersistent(configRoot);
        }
        if (configProperties == null) {
            configProperties = this.getAllConfigs();
        }
    }

    @Override
    public void reload() {
        List<String> configList = this.client.getChildren(configRoot);
        Map<String, String> tmpMap = new HashMap<String, String>();
        for(String config : configList){
            String value = this.client.readData(this.getConfigPath(config));
            tmpMap.put(config, value);
        }
        configProperties = tmpMap;
    }

    @Override
    public void add(String key, String value) {
        String configPath = this.getConfigPath(key);
        this.client.createPersistent(configPath, value);
        configCenterWatcher.watch(configPath);
    }

    @Override
    public void update(String key, String value) {
        String configPath = this.getConfigPath(key);
        this.client.writeData(configPath, value);
        configCenterWatcher.watch(configPath);
    }

    @Override
    public void delete(String key) {
        String configPath = this.getConfigPath(key);
        this.client.delete(configPath);
    }

    @Override
    public String get(String key) {
        if(this.configProperties.get(key) == null){
            String configPath = this.getConfigPath(key);
            if(!this.client.exists(configPath)){
                return null;
            }
            return this.client.readData(configPath);
        }
        return configProperties.get(key);
    }

    @Override
    public Map<String, String> getAllConfigs() {
        if(configProperties != null){
            return configProperties;
        }
        List<String> configList = this.client.getChildren(configRoot);
        Map<String, String> currentConfigProperties = new HashMap<String, String>();
        for(String config : configList){
            String value = this.client.readData(config);
            String key = config.substring(config.indexOf("/")+1);
            currentConfigProperties.put(key, value);
        }
        return currentConfigProperties;
    }

    private String getConfigPath(String key){
        return configRoot.concat("/").concat(key);
    }
}
