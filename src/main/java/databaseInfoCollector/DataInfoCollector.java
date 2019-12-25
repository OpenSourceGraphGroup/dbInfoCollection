package databaseInfoCollector;

import common.Common;

import java.sql.*;
import java.util.List;

/**
 * @Author: Jiaye Liu
 * @Description: 数据统计信息采集
 **/
class DataInfoCollector {
    private Connection connection = null;
    DataInfoCollector(Connection conn) {
        try {
            connection = conn;
            conn.getMetaData();
            conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    String getDataStatistics(long tableSize, List<Object> columns) {
        StringBuilder result = new StringBuilder();
        for (Object c : columns) {
            Column tmp = (Column) c;
            String dataInfo = "";
            switch (tmp.columnType.toUpperCase()) {
                case "INTEGER":
                    dataInfo += getInteger(tableSize, tmp);
                    break;
                case "VARCHAR":
                    dataInfo += getVarchar(tableSize, tmp);
                    break;
                case "BOOL":
                    dataInfo += getBool(tableSize, tmp);
                    break;
                case "REAL":
                case "DECIMAL":
                case "DATETIME":
                case "DATE":
                    dataInfo += getReal(tableSize, tmp);
            }
            result.append(dataInfo).append("\n");
//            System.out.println(dataInfo);
        }
        return result.toString().toUpperCase();
    }

    private String getInteger(long tableSize, Column c) {
        double nullRatio = getNullRatio(tableSize, c);
        long cardinality = getLongValue(c.getDistinctSizeSQL());
        String maxValue = getStringValue(c.getMaxValueSQL());
        String minValue = getStringValue(c.getMinValueSQL());

        // result
        return String.format("D[%s.%s; %.3f; %d; %s; %s]", c.tableName, c.columnName, nullRatio, cardinality, minValue, maxValue);
    }

    private String getReal(long tableSize, Column c) {
        double nullRatio = getNullRatio(tableSize, c);
        String maxValue = getStringValue(c.getMaxValueSQL());
        String minValue = getStringValue(c.getMinValueSQL());

        // result
        return String.format("D[%s.%s; %.3f; %s; %s]", c.tableName, c.columnName, nullRatio, minValue, maxValue);
    }

    private String getVarchar(long tableSize, Column c) {
        double nullRatio = getNullRatio(tableSize, c);
        String avgLength = getStringValue(c.getAvgLengthSQL());
        long maxLength = getLongValue(c.getMaxLengthSQL());

        // result
        return String.format("D[%s.%s; %.3f; %s; %d]", c.tableName, c.columnName, nullRatio, avgLength, maxLength);
    }

    private String getBool(long tableSize, Column c) {
        // null ratio
        double nullRatio = getNullRatio(tableSize, c);

        // true ratio
        long trueSize = getLongValue(c.getTrueSizeSQL());
        long notNullSize = getLongValue(c.getNotNullSizeSQL());
        double trueRatio = notNullSize <= 0 ? 0 : trueSize / (double) notNullSize;

        // result
        return String.format("D[%s.%s; %.3f; %.3f]", c.tableName, c.columnName, nullRatio, trueRatio);
    }

    private double getNullRatio(long tableSize, Column c) {
        long nullSize = getLongValue(c.getNullSizeSQL());
        return tableSize <= 0 ? 0 : nullSize / (double) tableSize;
    }

    private String getStringValue(String sql) {
        ResultSet rs = Common.query(connection, sql);
        try {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
    }

    private long getLongValue(String sql) {
        ResultSet rs = Common.query(connection, sql);
        try {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

}

class Column {
    private String schemaName;
    String tableName;
    String columnName;
    String columnType;

    Column(String schemaName, String tableName, String columnName, String columnType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
    }

    String getDistinctSizeSQL() {
        return String.format("select COUNT(DISTINCT %s) from %s.%s", columnName, schemaName, tableName);
    }

    String getMaxValueSQL() {
        return String.format("select MAX(%s) from %s.%s", columnName, schemaName, tableName);
    }

    String getMinValueSQL() {
        return String.format("select MIN(%s) from %s.%s", columnName, schemaName, tableName);
    }

    String getNullSizeSQL() {
        return String.format("select COUNT(*) from %s.%s where %s is null", schemaName, tableName, columnName);
    }

    String getNotNullSizeSQL() {
        return String.format("select COUNT(*) from %s.%s where %s is not null", schemaName, tableName, columnName);
    }

    String getTrueSizeSQL() {
        return String.format("select COUNT(*) from %s.%s where %s = true", schemaName, tableName, columnName);
    }

    String getMaxLengthSQL() {
        return String.format("select MAX(LENGTH(%s)) from %s.%s", columnName, schemaName, tableName);
    }

    String getAvgLengthSQL() {
        return String.format("select AVG(LENGTH(%s)) from %s.%s", columnName, schemaName, tableName);
    }

    @Override
    public String toString() {
        String result = "";
        result += columnName + "," + columnType;
        return result;
    }
}