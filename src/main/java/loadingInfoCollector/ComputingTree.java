package loadingInfoCollector;

import common.Common;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Ran Wang
 * @Description: Compute intermediate results using query tree
 */
class ComputingTree {

    private static void getAllInformation(List<String> leafInfo, List<String> selectInfo, List<String> joinInfo, QueryNode node) {
        QueryNode leftChild = node.leftChild;
        QueryNode rightChild = node.rightChild;
        if (leftChild != null) {
            getAllInformation(leafInfo, selectInfo, joinInfo, leftChild);
        }
        if (rightChild != null && !(node.nodeType == NodeType.JOIN_NODE && !node.condition.contains("="))) {
            getAllInformation(leafInfo, selectInfo, joinInfo, rightChild);
        }
        String info = node.condition;
        if (node.nodeType == NodeType.LEAF_NODE) {
            int i = leafInfo.indexOf(info);
            if (i >= 0) {
                String replace_name = info.charAt(0) + String.valueOf(i);
                leafInfo.set(i, info + " " + replace_name);
                for (int s = 0; s < selectInfo.size(); s++) {
                    String old_select = selectInfo.get(s);
                    String new_select = old_select.replace(info, replace_name);
                    selectInfo.set(s, new_select);
                }
                for (int j = 0; j < joinInfo.size(); j++) {
                    String old_join = joinInfo.get(j);
                    String new_join = old_join.replace(info, replace_name);
                    joinInfo.set(j, new_join);
                }
            }
            leafInfo.add(info);
        } else if (node.nodeType == NodeType.SELECT_NODE) {
            selectInfo.add(info);
        } else {
            joinInfo.add(info);
        }
    }

    private static String computingNode(QueryNode joinNode) {
        List<String> leafInfo = new ArrayList<>();
        List<String> selectInfo = new ArrayList<>();
        List<String> joinInfo = new ArrayList<>();
        getAllInformation(leafInfo, selectInfo, joinInfo, joinNode);
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

    private static void updateCount(Connection connection, QueryNode node, String sql) throws SQLException {
        ResultSet resultSet = Common.query(connection, sql);
        int count = 0;
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }
        node.count = count;
        node.sql = sql;
    }

    static void computingSqlUpdateCount(Connection connection, QueryNode root) {
        if (root != null)
            root.postOrder(queryNode -> {
                String sql = computingNode(root);
                try {
                    updateCount(connection, root, sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                Common.info("loadingInfoCollector.ComputingTree:\r\n");
                printInfo(root);
            });
    }

    private static void printInfo(QueryNode node) {
        node.postOrder(queryNode -> {
            NodeType type = node.nodeType;
            String sql = node.sql;
            String condition = node.condition;
            int count = node.count;
            Common.info("Node " + "#type: " + type + " #sql: " + sql + "\r\n" +
                    "\tNode " + "#condi: " + condition + " #count: " + count + "\r\n");
        });
    }

    private static String combineList(List<String> list, String sep) {
        StringBuilder result = new StringBuilder();
        if (list.size() <= 0) {
            return result.toString();
        }
        int i = 0;
        for (; i < list.size() - 1; i++) {
            result.append(list.get(i)).append(sep);
        }
        result.append(list.get(i));
        return result.toString();
    }
}
