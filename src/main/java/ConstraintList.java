
import java.sql.Connection;
import java.util.*;

public class ConstraintList {
    private Connection connection;
    private JoinInfo joinInfo;
    private SelectInfo selectInfo;
    private Map<String, Integer> tableMap = new HashMap<>();
    private List<String> tableConstraints = new LinkedList<>();
    private int index = 0;

    private Map<Integer, Integer> joinCount = new HashMap<>();

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public JoinInfo getJoinInfo() {
        return joinInfo;
    }

    public void setJoinInfo(JoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    public SelectInfo getSelectInfo() {
        return selectInfo;
    }

    public void setSelectInfo(SelectInfo selectInfo) {
        this.selectInfo = selectInfo;
    }

    public Map<String, Integer> getTableMap() {
        return tableMap;
    }

    public void setTableMap(Map<String, Integer> tableMap) {
        this.tableMap = tableMap;
    }

    public List<String> getTableConstraints() {
        return tableConstraints;
    }

    public void setTableConstraints(List<String> tableConstraints) {
        this.tableConstraints = tableConstraints;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Map<Integer, Integer> getJoinCount() {
        return joinCount;
    }

    public void setJoinCount(Map<Integer, Integer> joinCount) {
        this.joinCount = joinCount;
    }

    public ConstraintList(Connection connection) {
        this.connection = connection;
        joinInfo = new JoinInfo(this.connection);
        selectInfo = new SelectInfo();
    }

    public List<String> getConstraintList(QueryNode root) throws Exception {
        if (root == null) {
            return new ArrayList<>();
        }

        List<QueryNode> queryNodeList = new ArrayList<>();
        root.postOrderNodes(queryNodeList);

        for (QueryNode node : queryNodeList) {
            double filterRate = 0;
            if (node.parent != null && node.parent.count != 0) {
                filterRate = (double) node.parent.count / (double) node.count;
            }
            if (node.nodeType == NodeType.LEAF_NODE) {
                // leaf node's condition is the table name
                if (node.parent != null) {
                    if (node.parent.nodeType == NodeType.SELECT_NODE) {
                        parseParentSelect(node, filterRate);
                    } else {
                        // parent is a join node, I need to know the table name, and if there exists any attributes
                        // participating in this join as primary key or foreign key
                        parseParentJoin(node, filterRate);
                    }
                }
            } else if (node.nodeType == NodeType.SELECT_NODE) {
                if (node.parent != null) {
                    if (node.parent.nodeType == NodeType.JOIN_NODE) {
                        // I need to know the table name, and if there exists any attributes participating in this
                        // join as primary key or foreigin key
                        parseParentJoin(node, filterRate);
                    } else {
                        System.out.println("I think there must be sth wrong with the parsed tree, where there exists " +
                                "a SELECT NODE whose parent node is also a SELECT NODE");
                    }
                }
            } else if (node.nodeType == NodeType.JOIN_NODE) {
                if (node.parent != null) {
                    if (node.parent.nodeType == NodeType.JOIN_NODE) {
                        parseParentJoin(node, filterRate);
                    } else if (node.parent.nodeType == NodeType.SELECT_NODE) {
                        parseParentSelect(node, filterRate);
                    } else {
                        System.out.println("I think there must be sth wrong with the parsed tree, where there exists " +
                                "a JOIN NODE whose parent node is a LEAF NODE");
                    }
                }
            }
        }
        return tableConstraints;
    }

    private void parseParentSelect(QueryNode node, double filterRate) throws Exception {
        selectInfo.parseSelectNodeWhereOps(node.parent.condition);
        String whereOps = selectInfo.getParsedWhereOps();
        String tableName = selectInfo.getTableName();
        int idx = updateIdx(tableName);
        String tableConstraintStr = tableConstraints.get(idx) + "; [0, " + whereOps +
                ", " + filterRate + "]";
        tableConstraints.set(idx, tableConstraintStr);
    }

    private void parseParentJoin(QueryNode node, double filterRate) throws Exception {
        joinInfo.parseJoinInfo(node.parent.condition);
        String tableOneName = joinInfo.getTableOne();
        String tableTwoName = joinInfo.getTableTwo();
        int idxOne = updateIdx(tableOneName);
        int idxTwo = updateIdx(tableTwoName);
        String tableOneConstraintStr = tableConstraints.get(idxOne);
        String tableTwoConstraintStr = tableConstraints.get(idxTwo);
        if (joinInfo.isTableOneUsingPK()) {
            tableOneConstraintStr = updateConstraintStr(idxOne, tableOneConstraintStr, true);
            int start = 2 * (joinCount.get(idxOne) - 1);

            tableTwoConstraintStr += "; [2, " + joinInfo.getTableTwoJoinAttribute() + ", "
                    + filterRate + ", " + joinInfo.getTableOne() + "." + joinInfo.getTableOneJoinAttribute() +
                    ", " + (int) (Math.pow(2, start)) + ", " + (int) (Math.pow(2, start + 1)) + "]";
        } else {
            tableTwoConstraintStr = updateConstraintStr(idxTwo, tableTwoConstraintStr, false);
            int start = 2 * (joinCount.get(idxTwo) - 1);

            tableOneConstraintStr += "; [2, " + joinInfo.getTableOneJoinAttribute() + ", "
                    + filterRate + ", " + joinInfo.getTableTwo() + "." + joinInfo.getTableTwoJoinAttribute() +
                    ", " + (int) (Math.pow(2, start)) + ", " + (int) (Math.pow(2, start + 1)) + "]";
        }
        tableConstraints.set(idxOne, tableOneConstraintStr);
        tableConstraints.set(idxTwo, tableTwoConstraintStr);
    }

    private String updateConstraintStr(int idx, String tableConstraintStr, boolean isFirstTable) {
        if (!joinCount.containsKey(idx)) {
            joinCount.put(idx, 1);
            String attribute;
            if (isFirstTable) {
                attribute = joinInfo.getTableOneJoinAttribute();
            } else {
                attribute = joinInfo.getTableTwoJoinAttribute();
            }
            tableConstraintStr += "; [1, " + attribute + ", 1, 2]";
        } else {
            int count = joinCount.get(idx);
            count++;
            joinCount.put(idx, count);
            tableConstraintStr = tableConstraintStr.substring(0, tableConstraintStr.length() - 1);

            int start = 2 * (count - 1);
            tableConstraintStr += ", " + (int) (Math.pow(2, start)) + ", " + (int) (Math.pow(2, start + 1)) + "]";
        }
        return tableConstraintStr;
    }

    private int updateIdx(String tableName) {
        int idx = tableMap.getOrDefault(tableName, index);
        if (idx == index) {
            tableMap.put(tableName, idx);
            tableConstraints.add("[" + tableName + "]");
            index++;
        }
        return idx;
    }

}
