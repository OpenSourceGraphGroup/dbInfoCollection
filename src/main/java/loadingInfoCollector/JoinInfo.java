package loadingInfoCollector;

import common.Common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

enum KeyType{
    PK, FK, PK_AND_FK, None
}

/**
 *  @Author: Zhengmin Lai
 *  @Description: Parse Constraint List of Join Node
 */
public class JoinInfo {
    private Connection connection;
    private String schema;
    private Map<String, List<String>> tableAttributeMap;
    private Map<String, KeyType> keyInfoMap;
    private Map<String, String> fkReferenceMap;
    private Map<String, String> tableNickNameMap;

    Map<String, String> getFkReferenceMap() {
        return fkReferenceMap;
    }

    public void setFkReferenceMap(Map<String, String> fkReferenceMap) {
        this.fkReferenceMap = fkReferenceMap;
    }

    Map<String, List<String>> getTableAttributeMap() {
        return tableAttributeMap;
    }

    public void setTableAttributeMap(Map<String, List<String>> tableAttributeMap) {
        this.tableAttributeMap = tableAttributeMap;
    }

    Map<String, KeyType> getKeyInfoMap() {
        return keyInfoMap;
    }

    public void setKeyInfoMap(Map<String, KeyType> keyInfoMap) {
        this.keyInfoMap = keyInfoMap;
    }

    JoinInfo(Connection connection, Map<String, String> tableNickNameMap){
        this.connection = connection;
        this.tableNickNameMap = tableNickNameMap;
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


    void parseJoinInfo(String condition) throws Exception{
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

                    KeyType tableOneKeyType = keyInfoMap.get(tableOneName);
                    KeyType tableTwoKeyType = keyInfoMap.get(tableTwoName);

                    parseKeyInfoMap(tableOneName, tableTwoKeyType, tableOneKeyType);
                    parseKeyInfoMap(tableTwoName, tableOneKeyType, tableTwoKeyType);

                    if (keyInfoMap.get(tableOneName) == KeyType.FK && keyInfoMap.get(tableTwoName) == KeyType.PK) {
                        fkReferenceMap.put(tableOneName + "." + tableOneJoinAttr, tableTwoName + "." + tableTwoJoinAttr);
                    } else if (keyInfoMap.get(tableTwoName) == KeyType.FK && keyInfoMap.get(tableOneName) == KeyType.PK) {
                        fkReferenceMap.put(tableTwoName + "." + tableTwoJoinAttr, tableOneName + "." + tableOneJoinAttr);
                    } else {
                        throw new Exception("Can not find index when joining two tables!");
                    }
                }
            }

        }
    }

    private void parseKeyInfoMap(String tableTwoName, KeyType tableOneKeyType, KeyType tableTwoKeyType) throws Exception {
        if (tableTwoKeyType == KeyType.PK_AND_FK) {
            if (tableOneKeyType == KeyType.FK) {
                keyInfoMap.put(tableTwoName, KeyType.PK);
            } else if (tableOneKeyType == KeyType.PK) {
                keyInfoMap.put(tableTwoName, KeyType.FK);
            } else {
                throw new Exception("Can not find index when joining two tables!");
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
        this.tableAttributeMap.forEach((curTableName, joinAttributes)->{
            for(String joinAttribute: joinAttributes) {

                String tableName = curTableName;
                if(tableNickNameMap.containsKey(curTableName)){
                    tableName = tableNickNameMap.get(curTableName);
                }

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
                    this.keyInfoMap.put(curTableName, KeyType.PK_AND_FK);
                }else if(isTableUsingPK){
                    this.keyInfoMap.put(curTableName, KeyType.PK);
                }else if(isTableUsingFK){
                    this.keyInfoMap.put(curTableName, KeyType.FK);
                }else{
                    this.keyInfoMap.put(curTableName, KeyType.None);
                }
            }
        });
    }
}
