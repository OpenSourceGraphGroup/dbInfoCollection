
import java.sql.Connection;
import java.util.*;

public class ConstraintList {
    private Connection connection;
    private Map<String, Integer> tableMap = new HashMap<>();
    private List<String> tableConstraints = new LinkedList<>();
    private Set<String> parsedConditions = new HashSet<>();
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

    private void parseParentSelect(QueryNode node, double filterRate) throws Exception {
        if(!ifConditionParsed(node.parent.condition)) {
            SelectInfo selectInfo = new SelectInfo();
            selectInfo.parseSelectNodeWhereOps(node.parent.condition);
            String whereOps = selectInfo.getParsedWhereOps();
            String tableName = selectInfo.getTableName();
            int idx = updateIdx(tableName);
            String tableConstraintStr = tableConstraints.get(idx) + "; [0, " + whereOps +
                    ", " + filterRate + "]";
            tableConstraints.set(idx, tableConstraintStr);
        }
    }

    private boolean ifConditionParsed(String condition){
        if(!this.parsedConditions.contains(condition)){
            this.parsedConditions.add(condition);
            return false;
        }else{
            return true;
        }
    }

    private void parseParentJoin(QueryNode node, double filterRate) throws Exception {
        if(!ifConditionParsed(node.parent.condition)) {
            JoinInfo joinInfo = new JoinInfo(connection);
            joinInfo.parseJoinInfo(node.parent.condition);

            Set<String> keySet = joinInfo.getTableAttributeMap().keySet();
            List<String> keys = new ArrayList<>(keySet);

            if (keys.size() != 2) {
                throw new Exception("Parse Join Node error!");
            }
            // we will parse join key table first to record the joinCount info which will be used later in fk join table
            if (joinInfo.getKeyInfoMap().get(keys.get(1)) == KeyType.PK) {
                String tmpKey = keys.get(0);
                keys.set(0, keys.get(1));
                keys.set(1, tmpKey);
            }
            int pkIdx = updateIdx(keys.get(0));
            for (int i = 0; i < 2; i++) {
                String tableName = keys.get(i);
                boolean isAnotherFK = true;
                if (i == 0) {
                    String anotherTableName = keys.get(1);
                    if (joinInfo.getKeyInfoMap().get(anotherTableName) != KeyType.FK) {
                        isAnotherFK = false;
                    }
                }
                int idx = updateIdx(tableName);
                List<String> attributes = joinInfo.getTableAttributeMap().get(tableName);
                String tableConstraintStr = tableConstraints.get(idx);
                if (joinInfo.getKeyInfoMap().containsKey(tableName)) {
                    if (joinInfo.getKeyInfoMap().get(tableName) == KeyType.PK && isAnotherFK) {
                        tableConstraintStr = updateConstraintStrPK(idx, attributes, tableConstraintStr);
                    } else {
                        tableConstraintStr = updateConstraintFK(filterRate, joinInfo, pkIdx, tableName, attributes, tableConstraintStr);
                    }
                    tableConstraints.set(idx, tableConstraintStr);
                } else {
                    System.out.println("Can not find table " + tableName + " in hashmap `isPrimaryKeyInfoMap`!");
                }
            }
        }
    }

    private String updateConstraintFK(double filterRate, JoinInfo joinInfo,
                                      int pkIdx, String tableName, List<String> attributes,
                                      String tableConstraintStr) {
        int start = 2 * (joinCount.get(pkIdx) - 1);
        StringBuilder fkAttributes = new StringBuilder();
        for (String attr : attributes) {
            fkAttributes.append(attr).append("#");
        }
        fkAttributes.deleteCharAt(fkAttributes.length() - 1);

        StringBuilder fkRefAttributes = new StringBuilder();
        for (String attr : attributes) {
            fkRefAttributes.append(joinInfo.getFkReferenceMap().get(tableName + "." + attr)).append("#");
        }
        fkRefAttributes.deleteCharAt(fkRefAttributes.length() - 1);

        tableConstraintStr += "; [2, " + fkAttributes + ", " + filterRate + ", " + fkRefAttributes +
                ", " + (int) (Math.pow(2, start)) + ", " + (int) (Math.pow(2, start + 1)) + "]";

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
