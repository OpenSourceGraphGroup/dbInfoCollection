import org.junit.Test;

import java.sql.Connection;
import java.util.*;

public class MyTest {

    @Test
    public void testPostOrderNodes(){
        QueryNode root = new QueryNode(NodeType.JOIN_NODE, "Condition 1");
        root.leftChild = new QueryNode(NodeType.SELECT_NODE, "Condition 2");
        root.leftChild.leftChild = new QueryNode(NodeType.LEAF_NODE, "Condition 3");
        root.rightChild = new QueryNode(NodeType.LEAF_NODE, "Condition 4");

        List<QueryNode> nodes = new ArrayList<>();
        root.postOrderNodes(nodes);

        for(QueryNode node : nodes){
            System.out.println("Node type: " + node.nodeType + ", condition: " + node.condition);
        }
    }

    @Test
    public void testSelectInfo() throws Exception{

        String condition = "((tpch.orders.O_ORDERDATE >= DATE'1997-01-01') or (tpch.orders.O_ORDERDATE < ((DATE'1997-01-01' + interval '1' year))))";

        SelectInfo selectInfo = new SelectInfo();
        selectInfo.parseSelectNodeWhereOps(condition);

        System.out.println(selectInfo.getParsedWhereOps());
        System.out.println(selectInfo.getTableName());
    }

    @Test
    public void testJoinInfo() throws Exception{
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        JoinInfo joinInfo = new JoinInfo(connection, new HashMap<>());
        String condition = "tpch.orders.O_CUSTKEY = tpch.customer.C_CUSTKEY ";
        joinInfo.parseJoinInfo(condition);
    }


    @Test
    public void testRefineConstraintList() throws Exception{
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        ConstraintList constraintList = new ConstraintList(connection);
        Map<String, Integer> tableMap = new HashMap<>();
        tableMap.put("A", 0);
        tableMap.put("B", 1);
        tableMap.put("C", 2);
        tableMap.put("D", 3);
        tableMap.put("E", 4);
        tableMap.put("F", 5);
        tableMap.put("G", 6);
        tableMap.put("H", 7);
        constraintList.setTableMap(tableMap);

        List<String> tableConstraints = new ArrayList<>();
        tableConstraints.add("[A]; [1, a, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]");
        tableConstraints.add("[B]; [2, a, 0.2, A.a, 1, 2]");
        tableConstraints.add("[C]; [2, a, 0.2, A.a, 4, 8]");
        tableConstraints.add("[D]; [2, a, 1, A.a, 16, 32]");
        tableConstraints.add("[E]; [2, a, 0.2, A.a, 64, 128]");
        tableConstraints.add("[F]; [2, a, 1, A.a, 256, 512]");
        tableConstraints.add("[G]; [1, g, 1, 2]");
        tableConstraints.add("[H]; [2, g, 1, G.g, 1, 2]");
        constraintList.setTableConstraints(tableConstraints);
        constraintList.refineConstraintList();
        for(String output: constraintList.getTableConstraints()){
            System.out.println(output);
        }
    }
}
