package databaseInfoCollector;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Jiaye Liu
 * @Description: 数据库表模式采集
 **/

class SchemaCollector {
    private DatabaseMetaData databaseMetaData = null;
    private Statement st = null;

    SchemaCollector(Connection conn) {
        try {
            databaseMetaData = conn.getMetaData();
            st = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库表名list
     *
     * @Description: getAllTable
     * @Param: [conn] 数据库连接
     * @return: java.util.List<java.lang.Object>
     */
    List<Object> getTableList(String dbName) {
        List<Object> tableNameList = new ArrayList<>();
        String[] types = {"TABLE"};
        try {
            ResultSet rs = databaseMetaData.getTables(dbName, null, "%", types);
            while (rs.next()) {
                tableNameList.add(rs.getString("TABLE_NAME"));
            }
            return tableNameList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取表数据量
     *
     * @Description: getTableSize
     * @Param: [schemaName, tableName]
     * @Returns: java.lang.String
     */
    String getTableSize(String schemaName, String tableName) {
        try {
            String sql = String.format("select count(*) from %s.%s", schemaName, tableName);
            ResultSet rs = st.executeQuery(sql);
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取主键信息
     *
     * @Description: getPrimaryKeys
     * @Param: [schemaName, tableName]
     * @return: List<Object>
     * @Author: Jiaye Liu
     * @Date: 9:46
     */
    private List<Object> getPrimaryKeys(String schemaName, String tableName) {
        List<Object> pkNameList = new ArrayList<>();
        try {
            ResultSet rs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);
            while (rs.next()) {
//                String columnName = rs.getString("PK_NAME");//主键名称
//                short keySeq = rs.getShort("KEY SEQ");//序列号
                String pkName = rs.getString("COLUMN_NAME");
                pkNameList.add(pkName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pkNameList;
    }

    /**
     * 将主键list转换为要求的String
     *
     * @Description: getPKNameListString
     * @Param: [pkNameList]
     * @return: java.lang.String -->P(primary_key,primary_key,...)
     * @Author: Jiaye Liu
     * @Date: 10:01
     */
    private String getPKNameListString(List<Object> pkNameList) {
        StringBuilder result = new StringBuilder("P(");
        for (Object i : pkNameList) {
            result.append((String) i);
            result.append(",");
        }
        result = new StringBuilder(result.substring(0, result.length() - 1));
        result.append(")");
        return result.toString().toUpperCase();
    }

    /**
     * @Description: getForeignKeys
     * @Param: [schemaName, tableName]
     * @return: java.lang.String -->F(foreign_key, referenced_table.referenced_primary_key)
     * @Author: Jiaye Liu
     * @Date: 9:54
     */
    private List<Object> getForeignKeys(String schemaName, String tableName) {
        List<Object> ekList = new ArrayList<>();
        try {
            ResultSet rs = databaseMetaData.getImportedKeys(null, schemaName, tableName);
            while (rs.next()) {
                String fkColumnName = rs.getString("FKCOLUMN_NAME");
                String pkTableName = rs.getString("PKTABLE_NAME");
                String pkColumnName = rs.getString("PKCOLUMN_NAME");

                ForeignKeys fk = new ForeignKeys(fkColumnName, pkTableName);
                fk.appendPKName(pkColumnName);
                ekList.add(fk);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ekList;
    }

    /**
     * 外键列表转为String
     *
     * @Description: getFKNameListString
     * @Param: [fkNameList]
     * @Returns: java.lang.String
     */
    private String getFKNameListString(List<Object> fkNameList) {
        StringBuilder result = new StringBuilder();
        for (Object i : fkNameList) {
            result.append("F(");
            result.append(i);
            result.append(");");
        }
        if (result.length() <= 0) {
            return "";
        } else {
            result = new StringBuilder(result.substring(0, result.length() - 1));
            return result.toString().toUpperCase();
        }
    }

    /**
     * 获取表的列名与数据类型信息
     *
     * @Description: getTableColumns
     * @Param: [schemaName, tableName]
     * @Returns: java.util.List<java.lang.Object>
     */
    List<Object> getTableColumns(String schemaName, String tableName) {
        List<Object> tableColumns = new ArrayList<>();
        try {
            ResultSet rs = databaseMetaData.getColumns(null, schemaName, tableName, "%");
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("TYPE_NAME");
                if (columnType.toUpperCase().equals("CHAR"))
                    columnType = "VARCHAR";
                if (columnType.toUpperCase().equals("INT"))
                    columnType = "INTEGER";
                Column c = new Column(schemaName, tableName, columnName, columnType);
                tableColumns.add(c);
            }
            return tableColumns;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 输出表的所有信息
     *
     * @Description: getTableInfo
     * @Param: [schemaName, tableName]
     * @Returns: java.lang.String
     */
    String getTableInfo(String schemaName, String tableName) {
        List<Object> columnList = getTableColumns(schemaName, tableName);
        List<Object> pkList = getPrimaryKeys(schemaName, tableName);
        List<Object> fkList = getForeignKeys(schemaName, tableName);
        StringBuilder result = new StringBuilder("T[");
        result.append(tableName).append(";").append(getTableSize(schemaName, tableName)).append(";");
        for (Object c : columnList) {
            Column tmp = (Column) c;
            result.append(tmp.toString()).append(";");
        }
        result.append(getPKNameListString(pkList)).append(";");
        result.append(getFKNameListString(fkList));
        result = new StringBuilder(result.substring(0, result.length() - 1));
        result.append(")]");
        return result.toString().toUpperCase();
    }

}

class ForeignKeys {
    private String fkName;
    private String fkTable;
    private List<Object> pkNameList;

    ForeignKeys(String fkName, String fkTable) {
        this.fkName = fkName;
        this.fkTable = fkTable;
        this.pkNameList = new ArrayList<>();
    }

    void appendPKName(String pkName) {
        this.pkNameList.add(pkName);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(fkName).append(",");
        for (Object s : pkNameList) {
            result.append(this.fkTable).append(".");
            result.append((String) s);
            result.append(",");
        }
        result = new StringBuilder(result.substring(0, result.length() - 1));
        return result.toString();
    }
}

