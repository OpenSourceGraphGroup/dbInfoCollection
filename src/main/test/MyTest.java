import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    public void testConstraintList() throws Exception{
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");

        QueryNode root = QueryTreeGenerator.generate(connection, Common.getSql("sql/16.sql"), "tpch");
        root.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
        ComputingTree ct = new ComputingTree();
        ct.computingSqlUpadteCount(connection, root);

        ConstraintList constraintList = new ConstraintList(connection);
        constraintList.getConstraintList(root);
        for(String output: constraintList.getTableConstraints()){
            System.out.println(output);
        }
    }
}
