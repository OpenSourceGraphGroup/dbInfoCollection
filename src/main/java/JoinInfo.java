
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinInfo {
    private Connection connection;
    private String schema;
    private Map<String, List<String>> tableAttributeMap;
    private Map<String, Boolean> isPrimaryKeyInfoMap;
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

    public Map<String, Boolean> getIsPrimaryKeyInfoMap() {
        return isPrimaryKeyInfoMap;
    }

    public void setIsPrimaryKeyInfoMap(Map<String, Boolean> isPrimaryKeyInfoMap) {
        this.isPrimaryKeyInfoMap = isPrimaryKeyInfoMap;
    }

    public JoinInfo(Connection connection){
        this.connection = connection;
        tableAttributeMap = new HashMap<>();
        isPrimaryKeyInfoMap = new HashMap<>();
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

                if(tableOneInfo.contains(".") && tableTwoInfo.contains(".")){
                    List<String> parsedInfoOne = parseTableInfo(tableOneInfo);
                    List<String> parsedInfoTWo = parseTableInfo(tableTwoInfo);

                    String tableOneName = parsedInfoOne.get(0);
                    String tableTwoName = parsedInfoTWo.get(0);
                    String tableOneJoinAttr = parsedInfoOne.get(1);
                    String tableTwoJoinAttr = parsedInfoTWo.get(1);

                    if(isPrimaryKeyInfoMap.get(tableOneName)){
                        fkReferenceMap.put(tableTwoName+"."+tableTwoJoinAttr, tableOneName + "." + tableOneJoinAttr);
                    }else{
                        fkReferenceMap.put(tableOneName+"."+tableOneJoinAttr, tableTwoName + "." + tableTwoJoinAttr);
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
                String sql = String.format("select count(*) from information_schema.STATISTICS where TABLE_SCHEMA='%s' " +
                                "and TABLE_NAME='%s' and INDEX_NAME='PRIMARY' and COLUMN_NAME='%s'", this.schema,
                        tableName, joinAttribute);
                ResultSet resultSet = Common.query(connection, sql);
                try {
                    if (resultSet.next()) {
                        int count = resultSet.getInt(1);
                        boolean isTableUsingPK = count != 0;
                        this.isPrimaryKeyInfoMap.put(tableName, isTableUsingPK);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
