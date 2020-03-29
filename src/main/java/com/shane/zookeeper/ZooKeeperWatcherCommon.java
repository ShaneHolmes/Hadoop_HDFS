package com.shane.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 这是一个自定义的监听器类.
 */

public class ZooKeeperWatcherCommon implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
        //获取事件类型
        Event.EventType eventType = watchedEvent.getType();
        //事件的路径
        String path = watchedEvent.getPath();
        //事件的状态
        Event.KeeperState state = watchedEvent.getState();
        /**
         *  第一： 如果 state 有值(SyncConnected=3)， 但是 type 和  path无值， 那就证明是在获取连接
         *  第二： type和path的意义就是告诉 监听器的 process 方法
         *  到底是哪个 path的那个事件被响应了。 以便process方法能够调用对应的业务逻辑的代码执行
         */

        System.out.println(path + "\t" + eventType  +  "\t" + state);

        if(path == null && state.getIntValue() == 3) {
            System.out.println("connect success!");
        }else {
            System.out.println(path + "节点的" + eventType + "事件被触发了");
        }

    }
}
