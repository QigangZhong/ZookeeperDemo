package com.qigang.zookeeper.democlient;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/**
 * 测试客户端
 */
public class ZookeeperClient implements Watcher {

    public static class ZkConfig {

        public static String connectString = "127.0.0.1:2181";
        public static int sessionTimeout = 30000;
        public static String authScheme = "digest";
        public static String accessKey = "cache:svcctlg";
        public static boolean authentication = false;
    }

    private ZooKeeper zk;

    /**
     * 创建zookeeper客户端
     *
     * @return
     */
    public boolean createZkClient() {
        try {
            zk = new ZooKeeper(ZkConfig.connectString, ZkConfig.sessionTimeout, this);
        } catch (IOException e) {
            this.log("{}", e);
            e.printStackTrace();
            return false;
        }
        if (ZkConfig.authentication) {
            zk.addAuthInfo(ZkConfig.authScheme, ZkConfig.accessKey.getBytes());
        }
        if (!isConnected()) {
            log(" ZooKeeper client state [{}]", zk.getState().toString());
        }
        try {
            if (zk.exists("/zookeeper", false) != null) {
                log("create ZooKeeper Client Success! connectString", ZkConfig.connectString);
                log(" ZooKeeper client state [{}]", zk.getState());
                return true;
            }
        } catch (Exception e) {
            this.log("create ZooKeeper Client Fail! connectString", ZkConfig.connectString);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 新增持久化节点
     *
     * @param path
     *            节点路径
     * @param data
     *            节点数据
     * @return
     */
    public boolean createPersistentNode(String path, String data) {
        if (isConnected()) {

            try {
                if (ZkConfig.authentication) {
                    zk.create(path, data.getBytes(), getAdminAcls(), CreateMode.PERSISTENT);
                } else {
                    zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                log("{}", e);
                return false;
            }

        }
        this.log("zookeeper state", zk.getState());
        return false;
    }

    /**
     * 创建瞬时节点
     *
     * @param path
     * @param data
     * @return
     */
    public boolean creatEphemeralNode(String path, String data) {
        if (isConnected()) {

            try {
                if (ZkConfig.authentication) {
                    zk.create(path, data.getBytes(), getAdminAcls(), CreateMode.PERSISTENT);
                } else {
                    zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                log("{}", e);
                return false;
            }

        }
        this.log("zookeeper state", zk.getState());
        return false;
    }

    /**
     * 修改数据
     *
     * @param path
     * @param data
     * @return
     */
    public boolean setNodeData(String path, String data) {
        if (isConnected()) {
            try {
                zk.setData(path, data.getBytes(), -1);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                this.log("{}", e);
                return false;
            }
        }
        this.log("zookeeper state = [{}]", zk.getState());
        return false;
    }

    /**
     * 删除节点
     *
     * @param path
     * @return
     */
    public boolean deleteNode(String path) {
        if (isConnected()) {
            try {
                zk.delete(path, -1);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                this.log("{}", e);
                return false;
            }
        }
        this.log("zookeeper state = [{}]", zk.getState());
        return false;
    }

    /**
     * 获取节点值
     *
     * @param path
     * @return
     */
    public String getNodeData(String path) {
        if (isConnected()) {
            String data = null;
            try {
                byte[] byteData = zk.getData(path, true, null);
                data = new String(byteData, "utf-8");
                return data;
            } catch (Exception e) {

                e.printStackTrace();
                this.log("{}", e);
                return null;
            }
        }
        this.log("zookeeper state = [{}]", zk.getState());
        return null;
    }

    /**
     * 获取path子节点名列表
     *
     * @param path
     * @return
     */
    public List<String> getChildren(String path) {
        if (isConnected()) {
            String data = null;
            try {
                return zk.getChildren(path, false);
            } catch (Exception e) {
                e.printStackTrace();
                this.log("{}", e);
                return null;
            }
        }
        this.log("zookeeper state = [{}]", zk.getState());
        return null;
    }

    /**
     * zookeeper是否连接服务器
     *
     * @return
     */
    public boolean isConnected() {
        return zk.getState().isConnected();
    }

    /**
     * 是否存在path路径节点
     *
     * @param path
     * @return
     */
    public boolean exists(String path) {
        try {
            return zk.exists(path, false) != null;
        } catch (Exception e) {

            this.log("{}", e);
        }
        return false;
    }

    /**
     * 关闭zookeeper
     */
    public void closeZk() {
        if (isConnected()) {
            try {
                zk.close();
                this.log("close zookeeper [{}]", "success");
            } catch (InterruptedException e) {
                this.log("zookeeper state = [{}]", e);
                e.printStackTrace();
            }
        } else {
            this.log("zookeeper state = [{}]", zk.getState());
        }

    }

    /**
     *
     * @return
     */
    public List<ACL> getCreateNodeAcls() {
        List<ACL> listAcls = new ArrayList<ACL>(3);
        try {
            Id id = new Id(ZkConfig.authScheme,
                    DigestAuthenticationProvider.generateDigest(ZkConfig.accessKey));
            ACL acl = new ACL(Perms.CREATE, id);
            listAcls.add(acl);

        } catch (NoSuchAlgorithmException e) {

            e.printStackTrace();
            return Ids.OPEN_ACL_UNSAFE;
        }
        return listAcls;
    }

    public List<ACL> getAdminAcls() {
        List<ACL> listAcls = new ArrayList<ACL>(3);
        try {
            Id id = new Id(ZkConfig.authScheme,
                    DigestAuthenticationProvider.generateDigest(ZkConfig.accessKey));
            ACL acl = new ACL(Perms.ALL, id);
            listAcls.add(acl);

        } catch (NoSuchAlgorithmException e) {

            e.printStackTrace();
            return Ids.OPEN_ACL_UNSAFE;
        }
        return listAcls;
    }

    public void log(String format, Object args) {
        int index = format.indexOf("{");
        StringBuilder sb = new StringBuilder(format);
        sb.insert(index + 1, "%s");
        System.out.println(String.format(sb.toString(), args));
    }

    @Override
    public void process(WatchedEvent event) {

        if(event.getType() == Event.EventType.NodeCreated){
            System.out.println("节点创建,path:"+event.getPath());
        }

        if(event.getType() == Event.EventType.NodeDataChanged){
            System.out.println("节点数据被修改,path:"+event.getPath());
        }

        if(event.getType() == Event.EventType.NodeChildrenChanged){
            System.out.println("子节点被修改,path:"+event.getPath());
        }

        if(event.getType() == Event.EventType.NodeDeleted){
            System.out.println("节点被删除,path:"+event.getPath());
        }
    }

}