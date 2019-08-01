/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jaskey.consistenthash.sample;

import com.github.jaskey.consistenthash.ConsistentHashRouter;
import com.github.jaskey.consistenthash.Node;
import com.github.jaskey.consistenthash.VirtualNode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * a sample usage for routing a request to services based on requester ip
 */
public class MyServiceNode implements Node{
    private final String idc;
    private final String ip;
    private final int port;

    public String getIdc() {
        return idc;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public MyServiceNode(String idc, String ip, int port) {
        this.idc = idc;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String getKey() {
        return idc + "-"+ip+":"+port;
    }

    @Override
    public String toString(){
        return getKey();
    }

    public static void main(String[] args) {
        //initialize 4 service node
        MyServiceNode node1 = new MyServiceNode("A","127.0.0.1",8080);
        MyServiceNode node2 = new MyServiceNode("B","127.0.0.1",8081);
        MyServiceNode node3 = new MyServiceNode("C","127.0.0.1",8082);
        MyServiceNode node4 = new MyServiceNode("D","127.0.0.1",8084);
        MyServiceNode node5 = new MyServiceNode("E","127.0.0.1",8085);
        MyServiceNode node6 = new MyServiceNode("F","127.0.0.1",8086);
        MyServiceNode node7 = new MyServiceNode("G","127.0.0.1",8087);
        MyServiceNode node8 = new MyServiceNode("H","127.0.0.1",8088);
        //hash them to hash ring
        ConsistentHashRouter<MyServiceNode> consistentHashRouter =
                new ConsistentHashRouter<>(Arrays.asList(node1,node2,node3,node4,node5,node6,node7,node8),1);

        soutRingInfo(consistentHashRouter);

        ConsistentHashRouter<MyServiceNode> consistentHashRouterWith10Virtual =
                new ConsistentHashRouter<>(Arrays.asList(node1,node2,node3,node4,node5,node6,node7,node8),150);

        soutRingInfo(consistentHashRouterWith10Virtual);

        //we have 5 requester ip, we are trying them to route to one service node
        String requestIP1 = "192.168.0.1";
        String requestIP2 = "192.168.0.2";
        String requestIP3 = "192.168.0.3";
        String requestIP4 = "192.168.0.4";
        String requestIP5 = "192.168.0.5";
        String requestIP6 = "192.168.0.6";
        String requestIP7 = "192.168.0.7";
        String requestIP8 = "192.168.0.8";
        String requestIP9 = "192.168.0.9";
        String requestIP10 = "192.168.0.10";
//        System.out.println(String.format("%s(%s)", requestIP1, consistentHashRouter.getHashFunction().hash(requestIP1)));
//        System.out.println(String.format("%s(%s)", requestIP2, consistentHashRouter.getHashFunction().hash(requestIP2)));
//        System.out.println(String.format("%s(%s)", requestIP3, consistentHashRouter.getHashFunction().hash(requestIP3)));
//        System.out.println(String.format("%s(%s)", requestIP4, consistentHashRouter.getHashFunction().hash(requestIP4)));
//        System.out.println(String.format("%s(%s)", requestIP5, consistentHashRouter.getHashFunction().hash(requestIP5)));
//        System.out.println(String.format("%s(%s)", requestIP6, consistentHashRouter.getHashFunction().hash(requestIP6)));
//        System.out.println(String.format("%s(%s)", requestIP7, consistentHashRouter.getHashFunction().hash(requestIP7)));
//        System.out.println(String.format("%s(%s)", requestIP8, consistentHashRouter.getHashFunction().hash(requestIP8)));
//        System.out.println(String.format("%s(%s)", requestIP9, consistentHashRouter.getHashFunction().hash(requestIP9)));
//        System.out.println(String.format("%s(%s)", requestIP10, consistentHashRouter.getHashFunction().hash(requestIP10)));

//        goRoute(consistentHashRouter,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);
//        System.out.println("-===============================--");

//        goRoute(consistentHashRouterWith10Virtual,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);
//        MyServiceNode node9 = new MyServiceNode("I","127.0.0.1",8089);
//        consistentHashRouter.addNode(node9,10);
//        goRoute(consistentHashRouter,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);
//        consistentHashRouter.removeNode(node3);
//        goRoute(consistentHashRouter,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);

//        goRoute(consistentHashRouterWith10Virtual,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);
//        MyServiceNode node9 = new MyServiceNode("I","127.0.0.1",8089);
//        consistentHashRouterWith10Virtual.addNode(node9,10);
//        goRoute(consistentHashRouterWith10Virtual,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);
        consistentHashRouterWith10Virtual.removeNode(node3);
        goRoute(consistentHashRouterWith10Virtual,requestIP1,requestIP2,requestIP3,requestIP4,requestIP5,requestIP6,requestIP7,requestIP8,requestIP9,requestIP10);

    }

    private static void soutRingInfo(ConsistentHashRouter<MyServiceNode> consistentHashRouter) {
        Map<String, Long> percent = new HashMap<>();
        SortedMap<Long, VirtualNode<MyServiceNode>> ring = consistentHashRouter.getRing();
        //4294967295
        for(Long id : ring.keySet()){
            System.out.print(String.format("%s(%s) ", ring.get(id).getPhysicalNode().getIdc(), id));
        }
        System.out.println();

        Long cha = null;
        Long before = 0L;
        Long last = 0L;
        for(Long id : ring.keySet()){

            if(cha == null){
                cha = id;
            }
            cha = id - before;
            System.out.print(String.format("%s ", cha));
            before = id;

            if(percent.get(ring.get(id).getPhysicalNode().getIdc())==null){
                percent.put(ring.get(id).getPhysicalNode().getIdc(), cha);
            }else{
                long exist = percent.get(ring.get(id).getPhysicalNode().getIdc());
                percent.put(ring.get(id).getPhysicalNode().getIdc(), exist + cha);
            }
            last = id;
        }
        System.out.print(String.format("%s ", 4294967295L-last));
        long exist = percent.get(ring.get(ring.firstKey()).getPhysicalNode().getIdc());
        percent.put(ring.get(ring.firstKey()).getPhysicalNode().getIdc(), exist + 4294967295L - last);

        System.out.println();
        for(String idc : percent.keySet()){
            System.out.print(String.format("%s(%.2f", idc, 100*(float)percent.get(idc)/(float)4294967295L)+"%) ");
        }
        System.out.println("----------------------");

    }

    private static void goRoute(ConsistentHashRouter<MyServiceNode> consistentHashRouter ,String ... requestIps){
        for (String requestIp: requestIps) {
            System.out.println(requestIp + " is route to " + consistentHashRouter.routeNode(requestIp));
        }
    }
}
