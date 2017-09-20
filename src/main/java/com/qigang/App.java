package com.qigang;

import com.qigang.zookeeper.config.ConfigCenter;
import com.qigang.zookeeper.config.ConfigCenterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private final static Logger logger = LoggerFactory.getLogger(ConfigCenter.class);
    public static void main(String[] args) {

        //region Getting Started
        /*final int CLIENT_PORT=2181;
        final int connTimeout=30000;
        try {
            // 创建一个与服务器的连接
            ZooKeeper zk = new ZooKeeper("localhost:" + CLIENT_PORT, connTimeout, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");
                }
            });
            // 创建一个目录节点
            zk.create("/testRootPath", "testRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            // 创建一个子目录节点
            zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println(new String(zk.getData("/testRootPath",false,null)));
            // 取出子目录节点列表
            System.out.println(zk.getChildren("/testRootPath",true));
            // 修改子目录节点数据
            zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1);
            System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]");
            // 创建另外一个子目录节点
            zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null)));
            // 删除子目录节点
            zk.delete("/testRootPath/testChildPathTwo",-1);
            zk.delete("/testRootPath/testChildPathOne",-1);
            // 删除父目录节点
            zk.delete("/testRootPath",-1);
            // 关闭连接
            zk.close();

        }catch (Exception ex){
            ex.printStackTrace();
        }finally {

        }*/
        //endregion

        //region ZookeeperClient Test
        /*ZookeeperClient zkc = new ZookeeperClient();
        zkc.createZkClient();

        if (!zkc.exists("/windowcreate")) {

            zkc.createPersistentNode("/windowcreate", "windowcreate");
        }
        if (!zkc.exists("/windowcreate/value")) {
            System.out.println("not exists /windowcreate/value");

            zkc.createPersistentNode("/windowcreate/value", "A0431P001");
        }
        if (!zkc.exists("/windowcreate/valuetmp")) {
            System.out.println("not exists /windowcreate/valuetmp");
            zkc.creatEphemeralNode("/windowcreate/valuetmp", "A0431P002");
        }
        System.out.println(zkc.getNodeData("/zookeeper"));
        System.out.println(zkc.getChildren("/windowcreate"));
        System.out.println(zkc.getNodeData("/windowcreate/value"));
        System.out.println(zkc.getNodeData("/windowcreate/valuetmp"));
        zkc.setNodeData("/windowcreate/value", "A0431P003");
        System.out.println(zkc.getNodeData("/windowcreate/value"));
        zkc.deleteNode("/windowcreate/value");
        System.out.println(zkc.exists("/windowcreate/value"));
        zkc.closeZk();*/
        //endregion

        //region ConfigCenterTest
        ConfigCenterImpl configCenter = new ConfigCenterImpl("127.0.0.1:2181");
        configCenter.add("testKey1", "1");
        configCenter.add("testKey2", "2");
        configCenter.add("testKey3", "3");
        configCenter.add("testKey4", "4");
        configCenter.add("testKey5", "5");
        configCenter.add("testKey6", "6");
        logger.info("value is===>"+configCenter.get("testKey1"));
        logger.info("value is===>"+configCenter.get("testKey2"));
        logger.info("value is===>"+configCenter.get("testKey3"));
        logger.info("value is===>"+configCenter.get("testKey4"));
        logger.info("value is===>"+configCenter.get("testKey5"));
        logger.info("value is===>"+configCenter.get("testKey6"));
        configCenter.update("testKey6", "testKey6");
        logger.info("update testKey6 value is===>"+configCenter.get("testKey6"));
        configCenter.delete("testKey1");
        configCenter.delete("testKey2");
        configCenter.delete("testKey3");
        configCenter.delete("testKey4");
        configCenter.delete("testKey5");
        configCenter.delete("testKey6");
        //endregion

    }
}
