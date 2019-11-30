
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

enum KeyType{
    PK, FK, PK_AND_FK, None
}

public class JoinInfo {
    private Connection connection;
    private String schema;
    private Map<String, List<String>> tableAttributeMap;
    private Map<String, KeyType> keyInfoMap;
    private Map<String, String> fkReferenceMap;

    public Map<String, String> getFkReferenceMap() {
        return fkReferenceMap;
    }

    public void setFkReferenceMap(Map<String, String> fkReferenceMap) {
        this.fkReferenceMap = fkReferenceMap;
    }

    public Map<String, List<String>> getTableAttributeMap() {
        return tableAttributeMap;
    }

    public void setTableAttributeMap(Map<String, List<String>> tableAttributeMap) {
        this.tableAttributeMap = tableAttributeMap;
    }

    public Map<String, KeyType> getKeyInfoMap() {
        return keyInfoMap;
    }

    public void setKeyInfoMap(Map<String, KeyType> keyInfoMap) {
        this.keyInfoMap = keyInfoMap;
    }

    public JoinInfo(Connection connection){
        this.connection = connection;
        tableAttributeMap = new HashMap<>();
        keyInfoMap = new HashMap<>();
        fkReferenceMap = new HashMap<>();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }


    public void parseJoinInfo(String condition) throws Exception{
        if(condition != null && condition.contains(" = ")){
            String[] conditions = condition.split(" and ");
            for(String curCondition: conditions) {
                String tableOneInfo = curCondition.split(" = ")[0];
                String tableTwoInfo = curCondition.split(" = ")[1];
                parseTable(tableOneInfo);
                parseTable(tableTwoInfo);
            }
            parseKeyInfo();
            parseFkRefInfo(condition);
        }else{
            throw new Exception("Parse Join Node Error! Can not find join info in condition '" +
                    condition + "' when joining two tables");
        }
    }

    private void parseFkRefInfo(String condition) throws Exception {
        if(condition != null && condition.contains(" = ")) {
            String[] conditions = condition.split(" and ");
            for (String curCondition : conditions) {
                String tableOneInfo = curCondition.split(" = ")[0];
                String tableTwoInfo = curCondition.split(" = ")[1];

                if (tableOneInfo.contains(".") && tableTwoInfo.contains(".")) {
                    List<String> parsedInfoOne = parseTableInfo(tableOneInfo);
                    List<String> parsedInfoTWo = parseTableInfo(tableTwoInfo);

                    String tableOneName = parsedInfoOne.get(0);
                    String tableTwoName = parsedInfoTWo.get(0);
                    String tableOneJoinAttr = parsedInfoOne.get(1);
                    String tableTwoJoinAttr = parsedInfoTWo.get(1);

                    if (keyInfoMap.get(tableOneName) == KeyType.PK
                            && keyInfoMap.get(tableTwoName) == KeyType.FK) {
                        fkReferenceMap.put(tableTwoName + "." + tableTwoJoinAttr, tableOneName + "." + tableOneJoinAttr);
                    } else if (keyInfoMap.get(tableOneName) == KeyType.FK
                            && keyInfoMap.get(tableTwoName) == KeyType.PK) {
                        fkReferenceMap.put(tableOneName + "." + tableOneJoinAttr, tableTwoName + "." + tableTwoJoinAttr);
                    } else {
                        throw new Exception("Can not find index when joining two tables!");
                    }
                }
            }

        }
    }

    private List<String> parseTableInfo(String info) throws Exception{
        List<String> tableNameAndAttr = new ArrayList<>();
        int firstIdx = info.indexOf(".");
        int lastIdx = info.lastIndexOf(".");
        if (firstIdx == -1 || lastIdx == -1) {
            throw new Exception("There must be sth wrong with the format where it should be in 'DATABASE.TABLE.ATTRIBUTE'");
        }
        this.schema = info.substring(0, firstIdx);
        String tableName = info.substring(firstIdx + 1, lastIdx);
        String joinAttribute = info.substring(lastIdx + 1);
        tableNameAndAttr.add(tableName);
        tableNameAndAttr.add(joinAttribute);
        return tableNameAndAttr;
    }

    private void parseTable(String info) throws Exception{
        if(info.contains(".")) {
            List<String> tableInfo = parseTableInfo(info);
            String tableName = tableInfo.get(0);
            String joinAttribute = tableInfo.get(1);

            if (this.tableAttributeMap.containsKey(tableName)) {
                List<String> list = this.tableAttributeMap.get(tableName);
                list.add(joinAttribute);
            } else {
                List<String> list = new ArrayList<>();
                list.add(joinAttribute);
                this.tableAttributeMap.put(tableName, list);
            }
        }
    }

    private void parseKeyInfo() {
        this.tableAttributeMap.forEach((tableName, joinAttributes)->{
            for(String joinAttribute: joinAttributes) {

                boolean isTableUsingFK = false;
                try {
                    isTableUsingFK = Common.isFK(this.connection, this.schema, tableName, joinAttribute);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                boolean isTableUsingPK = false;
                try {
                    isTableUsingPK = Common.isPK(this.connection, this.schema, tableName, joinAttribute);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if(isTableUsingPK && isTableUsingFK){
                    this.keyInfoMap.put(tableName, KeyType.FK);
                }else if(isTableUsingPK){
                    this.keyInfoMap.put(tableName, KeyType.PK);
                }else if(isTableUsingFK){
                    this.keyInfoMap.put(tableName, KeyType.FK);
                }else{
                    this.keyInfoMap.put(tableName, KeyType.None);
                }
            }
        });
    }
}
