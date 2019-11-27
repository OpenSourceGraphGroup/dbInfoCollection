import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ComputingTree {

    public QueryNode test1() {
        QueryNode leafLineitem = new QueryNode(NodeType.LEAF_NODE, "lineitem");
        QueryNode selectLineitem = new QueryNode(NodeType.SELECT_NODE, "l_shipdate <= date '1998-12-01' - interval '64' day");

        leafLineitem.setParent(selectLineitem);
        selectLineitem.setLeftChild(leafLineitem);

        return selectLineitem;
    }

    public QueryNode test3() {
        QueryNode leafCustomer = new QueryNode(NodeType.LEAF_NODE, "customer");
        QueryNode selectCustomer = new QueryNode(NodeType.SELECT_NODE, "c_mktsegment = 'BUILDING'");
        QueryNode leafOrder = new QueryNode(NodeType.LEAF_NODE, "orders");
        QueryNode joinCustomerOrder = new QueryNode(NodeType.JOIN_NODE, "c_custkey = o_custkey");
        QueryNode selectOrder = new QueryNode(NodeType.SELECT_NODE, "o_orderdate < date '1995-03-15'");
        QueryNode leafLineitem = new QueryNode(NodeType.LEAF_NODE, "lineitem");
        QueryNode joinOrderLineitem = new QueryNode(NodeType.JOIN_NODE, "l_orderkey = o_orderkey");
        QueryNode selectLineitem = new QueryNode(NodeType.SELECT_NODE, "l_shipdate > date '1995-03-15'");

        leafCustomer.setParent(selectCustomer);
        leafOrder.setParent(joinCustomerOrder);
        selectCustomer.setParent(joinCustomerOrder);
        joinCustomerOrder.setParent(selectOrder);
        leafLineitem.setParent(joinOrderLineitem);
        selectOrder.setParent(joinOrderLineitem);
        joinOrderLineitem.setParent(selectLineitem);

        selectCustomer.setLeftChild(leafCustomer);
        joinCustomerOrder.setLeftChild(selectCustomer);
        selectOrder.setLeftChild(joinCustomerOrder);
        joinOrderLineitem.setLeftChild(selectOrder);
        selectLineitem.setLeftChild(joinOrderLineitem);

        joinCustomerOrder.setRightChild(leafOrder);
        joinOrderLineitem.setRightChild(leafLineitem);

        return selectLineitem;
    }

    public void getAllInfomation(List<String> leafInfo, List<String> selectInfo, List<String> joinInfo, QueryNode node) {
        QueryNode leftChild = node.getLeftChild();
        QueryNode rightChild = node.getRightChild();
        if (leftChild != null) {
            getAllInfomation(leafInfo, selectInfo, joinInfo, leftChild);
        }
        if (rightChild != null) {
            getAllInfomation(leafInfo, selectInfo, joinInfo, rightChild);
        }
        String info = node.getCondition();
        if (node.getNodeType() == NodeType.LEAF_NODE) {
            int i = leafInfo.indexOf(info);
            if(i >= 0){
                String replace_name = info.charAt(0) + String.valueOf(i);
                leafInfo.set(i, info + " " + replace_name);
                for(int s=0; s < selectInfo.size(); s++){
                    String old_select = selectInfo.get(s);
                    String new_select = old_select.replace(info, replace_name);
                    selectInfo.set(s, new_select);
                }
                for(int j=0; j < joinInfo.size(); j++){
                    String old_join = joinInfo.get(j);
                    String new_join = old_join.replace(info, replace_name);
                    joinInfo.set(j, new_join);
                }
            }
            leafInfo.add(info);
        } else if (node.getNodeType() == NodeType.SELECT_NODE) {
            selectInfo.add(info);
        } else {
            joinInfo.add(info);
        }
    }

    public String computingNode(QueryNode joinNode) {
        List<String> leafInfo = new ArrayList<>();
        List<String> selectInfo = new ArrayList<>();
        List<String> joinInfo = new ArrayList<>();
        getAllInfomation(leafInfo, selectInfo, joinInfo, joinNode);
        String joinTabels = combineList(leafInfo, " , ");
        String joinKeys = combineList(joinInfo, " and ");
        String selectInfos = combineList(selectInfo, " and ");
        String sql = "";
        if (!joinTabels.equals("")) {
            sql = sql + "select count(*) from " + joinTabels;
        }
        if (!joinKeys.equals("") || !selectInfos.equals("")) {
            sql = sql + " where ";
            if (!joinKeys.equals("")) {
                sql = sql + joinKeys;
                if (!selectInfos.equals("")) {
                    sql = sql + " and " + selectInfos;
                }
            } else {
                sql = sql + selectInfos;
            }
        }
        if (!sql.equals("")) sql += " ;";
        return sql;
    }

    public void updateCount(Connection connection, QueryNode node, String sql) throws SQLException {
        ResultSet resultSet = Common.query(connection, sql);
        int count = 0;
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }
        node.setCount(count);
        node.setSql(sql);
    }

    public void computingSqlUpadteCount(Connection connection, QueryNode root) throws SQLException {
        QueryNode leftChild = root.getLeftChild();
        QueryNode rightChild = root.getRightChild();
        if (leftChild != null) {
            computingSqlUpadteCount(connection, leftChild);
        }
        if (rightChild != null) {
            computingSqlUpadteCount(connection, rightChild);
        }
        String sql = computingNode(root);
        updateCount(connection, root, sql);
    }

    public void printInfo(QueryNode node) {
        QueryNode leftChild = node.getLeftChild();
        QueryNode rightChild = node.getRightChild();
        if (leftChild != null) {
            printInfo(leftChild);
        }
        if (rightChild != null) {
            printInfo(rightChild);
        }
        NodeType type = node.getNodeType();
        String sql = node.getSql();
        String condition = node.getCondition();
        int count = node.getCount();
        System.out.println("Node " + "#type: " + type + " #sql: " + sql);
        System.out.println("Node " + "#condi: " + condition + " #count: " + count);
        System.out.println();
    }

    String combineList(List<String> list, String sep) {
        String result = "";
        if (list.size() <= 0) {
            return result;
        }
        int i = 0;
        for (; i < list.size() - 1; i++) {
            result += list.get(i) + sep;
        }
        result += list.get(i);
        return result;
    }

    @Test
    public void testComputingTree() throws SQLException, JSQLParserException {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        QueryNode root = QueryTreeGenerator.generate(connection, Common.getSql("sql/15.sql"));
        root.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
        ComputingTree ct = new ComputingTree();
        ct.computingSqlUpadteCount(connection, root);
        ct.printInfo(root);
    }

}
