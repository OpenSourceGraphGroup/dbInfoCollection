
import java.sql.Connection;
import java.util.*;

/**
 * @Author: Zhengmin Lai
 * @Description: Parse Constraint List
 */
public class ConstraintList {
    private Connection connection;
    private Map<String, Integer> tableMap = new HashMap<>();
    private List<String> tableConstraints = new LinkedList<>();
    private Set<String> parsedConditions = new HashSet<>();
    private Map<String, String> tableNickNameMap = new HashMap<>();
    private int index = 0;

    private Map<Integer, Integer> joinCount = new HashMap<>();

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
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
    }

    public List<String> getConstraintList(QueryNode root) throws Exception {
        if (root == null) {
            return new ArrayList<>();
        }

        List<QueryNode> queryNodeList = new ArrayList<>();
        root.postOrderNodes(queryNodeList);

        for (QueryNode node : queryNodeList) {
            double filterRate = 0;
            if (node.parent != null && node.count != 0) {
                filterRate = (double) node.parent.count / (double) node.count;
            }
            if (node.nodeType == NodeType.LEAF_NODE) {
                parseLeafNodeCondition(node.condition);
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
                        parseParentJoin(node, filterRate);
                    } else {
                        throw new Exception("I think there must be sth wrong with the parsed tree, where there exists " +
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
                        throw new Exception("I think there must be sth wrong with the parsed tree, where there exists " +
                                "a JOIN NODE whose parent node is a LEAF NODE");
                    }
                }
            }
        }
        return tableConstraints;
    }

    private void parseLeafNodeCondition(String condition) {
        String[] tables = condition.split(" ");
        if (tables.length > 1) {
            tableNickNameMap.put(tables[1], tables[0]);
        }
    }

    private void parseParentSelect(QueryNode node, double filterRate) throws Exception {
        SelectInfo selectInfo = new SelectInfo();
        selectInfo.parseSelectNodeWhereOps(node.parent.condition);
        String whereOps = selectInfo.getParsedWhereOps();
        String tableName = selectInfo.getTableName();
        int idx = updateIdx(tableName);
        String tableConstraintStr = tableConstraints.get(idx) + "; [0, " + whereOps +
                ", " + filterRate + "]";
        tableConstraints.set(idx, tableConstraintStr);
    }

    private boolean isConditionParsed(String condition) {
        if (!this.parsedConditions.contains(condition)) {
            this.parsedConditions.add(condition);
            return false;
        } else {
            return true;
        }
    }

    private void parseParentJoin(QueryNode node, double filterRate) throws Exception {
        if (!isConditionParsed(node.parent.condition)) {
            JoinInfo joinInfo = new JoinInfo(connection, tableNickNameMap);
            joinInfo.parseJoinInfo(node.parent.condition);

            Set<String> keySet = joinInfo.getTableAttributeMap().keySet();
            List<String> keys = new ArrayList<>(keySet);

            List<String> sortedKeys = new LinkedList<>();

            if (keys.size() < 2) {
                throw new Exception("Parse Join Node error! Can not find two join keys!");
            }
            // we will parse join key table first to record the joinCount info which will be used later in fk join table
            for (int i = 0; i < keys.size(); i++) {
                if (joinInfo.getKeyInfoMap().get(keys.get(i)) == KeyType.PK) {
                    sortedKeys.add(0, keys.get(i));
                } else {
                    sortedKeys.add(keys.get(i));
                }
            }
            List<Integer> pkIdxes = new ArrayList<>();
            for (int i = 0; i < sortedKeys.size(); i++) {
                String tableName = sortedKeys.get(i);
                int idx = updateIdx(tableName);
                List<String> attributes = joinInfo.getTableAttributeMap().get(tableName);
                String tableConstraintStr = tableConstraints.get(idx);
                if (joinInfo.getKeyInfoMap().containsKey(tableName)) {
                    if (joinInfo.getKeyInfoMap().get(tableName) == KeyType.PK) {
                        int pkIdx = updateIdx(sortedKeys.get(i));
                        pkIdxes.add(pkIdx);
                        tableConstraintStr = updateConstraintStrPK(pkIdx, attributes, tableConstraintStr);
                    } else {
                        tableConstraintStr = updateConstraintFK(filterRate, joinInfo, pkIdxes, tableName, attributes, tableConstraintStr);
                    }
                    tableConstraints.set(idx, tableConstraintStr);
                } else {
                    System.out.println("Can not find table " + tableName + " in hashmap `isPrimaryKeyInfoMap`!");
                }
            }
        }
    }

    private String updateConstraintFK(double filterRate, JoinInfo joinInfo,
                                      List<Integer> pkIdxes, String tableName, List<String> attributes,
                                      String tableConstraintStr) {
        StringBuilder fkAttributes = new StringBuilder();
        for (String attr : attributes) {
            fkAttributes.append(attr).append("#");
        }
        fkAttributes.deleteCharAt(fkAttributes.length() - 1);

        StringBuilder fkRefAttributes = new StringBuilder();
        List<String> refTables = new ArrayList<>();
        for (String attr : attributes) {
            String refAttr = joinInfo.getFkReferenceMap().get(tableName + "." + attr);
            fkRefAttributes.append(refAttr).append("#");

            String table = refAttr.substring(0, refAttr.indexOf("."));
            refTables.add(table);
        }
        fkRefAttributes.deleteCharAt(fkRefAttributes.length() - 1);

        StringBuilder refNum = new StringBuilder();
        // correspond the table name with its ref key num
        for(String t: refTables) {
            for (int pkIdx : pkIdxes) {
                if (tableMap.get(t).equals(pkIdx)) {
                    int start = 2 * (joinCount.get(pkIdx) - 1);
                    refNum.append(", ").append((int) (Math.pow(2, start))).append(", ").append((int) (Math.pow(2, start + 1)));
                }
            }
        }
        tableConstraintStr += "; [2, " + fkAttributes + ", " + filterRate + ", " + fkRefAttributes + refNum + "]";
        return tableConstraintStr;
    }

    private String updateConstraintStrPK(int idx, List<String> attributes,
                                         String tableConstraintStr) {
        if (!joinCount.containsKey(idx)) {
            joinCount.put(idx, 1);
            StringBuilder attributesStr = new StringBuilder();
            for (String attr : attributes) {
                attributesStr.append(attr).append("#");
            }
            attributesStr.deleteCharAt(attributesStr.length() - 1);
            tableConstraintStr += "; [1, " + attributesStr + ", 1, 2]";
        } else {
            int count = joinCount.get(idx);
            count++;
            joinCount.put(idx, count);
            int pkConstraintIdx = tableConstraintStr.indexOf("[1,");
            String firstStr = tableConstraintStr.substring(0, pkConstraintIdx);
            String secondStr = tableConstraintStr.substring(pkConstraintIdx);

            // get cur constraint str
            String tmp = secondStr.split(";")[0];
            String curConstraintStr = tmp.substring(0, tmp.length() - 1);
            int start = 2 * (count - 1);
            curConstraintStr += ", " + (int) (Math.pow(2, start)) + ", " + (int) (Math.pow(2, start + 1)) + "]";


            // insert new pk attribute by replacing the original
            String attr = curConstraintStr.split(",")[1].trim();
            StringBuilder attrMut = new StringBuilder(attr);
            for (String attribute : attributes) {
                if (!attr.contains(attribute)) {
                    attrMut.append("#").append(attribute);
                }
            }
            curConstraintStr = curConstraintStr.replace(attr, attrMut);
            secondStr = secondStr.replace(tmp, curConstraintStr);
            tableConstraintStr = firstStr + secondStr;
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
