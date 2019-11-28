
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JoinInfo {
    private Connection connection;
    private String schema;
    private String tableOne;
    private String tableTwo;
    private String tableOneJoinAttribute;
    private String tableTwoJoinAttribute;
    private boolean isTableOneUsingPK = false;

    public JoinInfo(Connection connection){
        this.connection = connection;
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

    public String getTableOne() {
        return tableOne;
    }

    public void setTableOne(String tableOne) {
        this.tableOne = tableOne;
    }

    public String getTableTwo() {
        return tableTwo;
    }

    public void setTableTwo(String tableTwo) {
        this.tableTwo = tableTwo;
    }

    public String getTableOneJoinAttribute() {
        return tableOneJoinAttribute;
    }

    public void setTableOneJoinAttribute(String tableOneJoinAttribute) {
        this.tableOneJoinAttribute = tableOneJoinAttribute;
    }

    public String getTableTwoJoinAttribute() {
        return tableTwoJoinAttribute;
    }

    public void setTableTwoJoinAttribute(String tableTwoJoinAttribute) {
        this.tableTwoJoinAttribute = tableTwoJoinAttribute;
    }

    public boolean isTableOneUsingPK() {
        return isTableOneUsingPK;
    }

    public void setTableOneUsingPK(boolean tableOneUsingPK) {
        isTableOneUsingPK = tableOneUsingPK;
    }

    public void parseJoinInfo(String condition) throws Exception{
        if(condition != null && condition.contains(" = ")){
            String tableOneInfo = condition.split(" = " )[0];
            String tableTwoInfo = condition.split(" = " )[1];
            parseTable(tableOneInfo, true);
            parseTable(tableTwoInfo, false);
            parseKeyInfo();
        }else{
            throw new Exception("Parse Join Node Error! Can not find join info in condition '" +
                    condition + "' when joining two tables");
        }
    }

    private void parseTable(String info, boolean isFirstTable) throws Exception{
        if(info.contains(".")){
            int firstIdx = info.indexOf(".");
            int lastIdx = info.lastIndexOf(".");
            if (firstIdx == -1 || lastIdx == -1) {
                throw new Exception("There must be sth wrong with the format where it should be in 'DATABASE.TABLE.ATTRIBUTE'");
            }
            if(isFirstTable) {
                this.schema  = info.substring(0, firstIdx);
                this.tableOne = info.substring(firstIdx+1, lastIdx);
                this.tableOneJoinAttribute = info.substring(lastIdx + 1);
            }else{
                this.tableTwo = info.substring(firstIdx+1, lastIdx);
                this.tableTwoJoinAttribute = info.substring(lastIdx + 1);
            }
        }
    }

    private void parseKeyInfo() {
        String sql = String.format("select count(*) from information_schema.STATISTICS where TABLE_SCHEMA='%s' " +
                "and TABLE_NAME='%s' and INDEX_NAME= 'PRIMARY' and COLUMN_NAME='%s'", this.schema,
                this.tableOne, this.tableOneJoinAttribute);
        ResultSet resultSet = Common.query(connection, sql);
        try {
            if (resultSet.next()) {
                int count  = resultSet.getInt(1);
                this.isTableOneUsingPK = count != 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
