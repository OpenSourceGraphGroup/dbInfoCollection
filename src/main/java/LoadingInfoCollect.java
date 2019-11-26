import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void main(String[] args) {

    }


    // Generate query tree according to query plan
    private static QueryNode queryTreeGenerate(Connection connection, String sql) {
        List<HashMap<String, String>> queryPlan = queryPlanGenerate(connection, sql);
        QueryNode start = new QueryNode(NodeType.LEAF_NODE);
        QueryNode currentNode = start;
        HashMap<String, String> plan = queryPlan.get(0);
        start.condition = plan.get("table");
        if (plan.get("Extra").contains("Using where")) {
            currentNode.parent = new QueryNode(NodeType.SELECT_NODE);
            currentNode.parent.leftChild = currentNode;
            currentNode.parent.condition = "";
            currentNode = currentNode.parent;
        }
        for (int index = 1; index < queryPlan.size(); index++) {
            plan = queryPlan.get(index);
            String[] joinKey = plan.get("ref").split("\\.");
            String leftJoinKey = joinKey[joinKey.length - 1];
            String tableName = plan.get("table");
            currentNode.parent = new QueryNode(NodeType.JOIN_NODE);
            currentNode.parent.condition = leftJoinKey + " = "
                    + getJoinKey(connection, plan.get("key"), tableName);
            currentNode.parent.leftChild = currentNode;
            currentNode.parent.rightChild = new QueryNode(NodeType.LEAF_NODE);
            currentNode.parent.rightChild.condition = tableName;
            currentNode = currentNode.parent;
            if (plan.get("Extra") != null && plan.get("Extra").contains("Using where")) {
                currentNode.parent = new QueryNode(NodeType.SELECT_NODE);
                currentNode.parent.leftChild = currentNode;
                currentNode.parent.condition = "";
                currentNode = currentNode.parent;
            }
        }
        return start;
    }

    // Get join key in 'tableName' whose index column is 'indexName'
    private static String getJoinKey(Connection connection, String indexName, String tableName) {
        String sql = String.format("select COLUMN_NAME from INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = '%s'", indexName);
        if (indexName.equals("PRIMARY")) {
            sql += String.format(" and TABLE_NAME = '%s'", tableName);
        }
        ResultSet resultSet = Common.query(connection, sql);
        try {
            if (resultSet.next())
                return resultSet.getString("COLUMN_NAME");
            else
                return "";
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
    }

    // Using 'explain' statement in mysql to generate query plan
    private static List<HashMap<String, String>> queryPlanGenerate(Connection connection, String _sql) {
        List<HashMap<String, String>> queryPlan = new ArrayList<>();
        String sql = String.format("explain %s", _sql);
        ResultSet resultSet = Common.query(connection, sql);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                HashMap<String, String> rowData = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object val = resultSet.getObject(i);
                    rowData.put(metaData.getColumnName(i), val == null ? "null" : val.toString());
                }
                if(queryPlan.size() ==0)
                    System.out.println(rowData.keySet());
                System.out.println(rowData.values());
                queryPlan.add(rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return queryPlan;
    }

    @Test
    public void testQueryTreeGenerate() throws SQLException {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        QueryNode start = queryTreeGenerate(connection, Common.getSql("sql/7.sql"));
        while (start != null) {
            System.out.println(start.nodeType + " " + start.condition);
            start = start.parent;
        }
        connection.close();
    }
}
