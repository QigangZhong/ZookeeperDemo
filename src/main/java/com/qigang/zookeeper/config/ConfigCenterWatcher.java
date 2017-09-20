package com.qigang.zookeeper.config;

import java.util.List;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigCenterWatcher {
    private final static Logger logger = LoggerFactory
            .getLogger(ConfigCenterWatcher.class);

    private ZkClient client;

    private ConfigCenterListener configCenterListener;

    private ConfigCenter configCenter;

    public ConfigCenterWatcher(ZkClient client, ConfigCenter configCenter) {
        this.client = client;
        this.configCenter = configCenter;
        configCenterListener = new ConfigCenterListener();
    }

    public void watch(String key){
        client.subscribeDataChanges(key, configCenterListener);
        client.subscribeChildChanges(key, configCenterListener);
    }

    private class ConfigCenterListener implements IZkDataListener,IZkChildListener{
        public void handleDataChange(String dataPath, Object data)
                throws Exception {
            logger.info("data {} change,start reload configProperties",dataPath);
            configCenter.reload();
        }

        public void handleDataDeleted(String dataPath) throws Exception {
            logger.info("data {} delete,start reload configProperties",dataPath);
            configCenter.reload();
        }

        public void handleChildChange(String parentPath,
                                      List<String> currentChilds) throws Exception {
            logger.info("data {} ChildChange,start reload configProperties",parentPath);
            configCenter.reload();
        }

    }
}