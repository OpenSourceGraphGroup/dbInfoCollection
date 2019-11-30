import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: dbInfoCollection
 * @description: 数据库表模式采集
 * @author: Jiaye Liu
 * @create: 2019-11-30 08:26
 **/

public class SchemaCollector {
    private DatabaseMetaData databaseMetaData = null;

    public SchemaCollector(Connection conn) {
        try {
            databaseMetaData = (DatabaseMetaData) conn.getMetaData();
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
    public List<Object> getAllTable() {
        List<Object> tableNameList = new ArrayList<Object>();
        try {
            ResultSet rs = databaseMetaData.getTables("", "", null, null);
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
     * 获取主键信息
     *
     * @Description: getPrimaryKeys
     * @Param: [schemaName, tableName]
     * @return: List<Object>
     * @Author: Jiaye Liu
     * @Date: 9:46
     */
    public List<Object> getPrimaryKeys(String schemaName, String tableName) {
        List<Object> pkNameList = new ArrayList<Object>();
//        String result="P(";
        try {
            ResultSet rs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);
            while (rs.next()) {
                String columnName = rs.getString("COLUMN NAME");//列名
                short keySeq = rs.getShort("KEY SEQ");//序列号
                String pkName = rs.getString("PK NAME");//主键名称
                pkNameList.add(pkName);
            }
//            for(String i:pkNameList){
//                result+=i;
//                result+=",";
//            }
//            result=result.substring(0,result.length()-1);
//            result+=")";
            return pkNameList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 将主键list转换为要求的String
    * @Description: getPKNameListString
    * @Param: [pkNameList]
    * @return: java.lang.String -->P(primary_key,primary_key,...)
    * @Author: Jiaye Liu
    * @Date: 10:01
    */
    public String getPKNameListString(List<Object> pkNameList) {
        String result = "P(";
        for (Object i: pkNameList) {
            result+=(String)i;
            result+=",";
        }
        result=result.substring(0,result.length()-1);result=result.substring(0,result.length()-1);
        result+=")";
        return result;
    }

    /**
     * @Description: getExportedKeys
     * @Param: [schemaName, tableName]
     * @return: java.lang.String -->F(foreign_key, referenced_table.referenced_primary_key)
     * @Author: Jiaye Liu
     * @Date: 9:54
     */
    public void getExportedKeys(String schemaName, String tableName) {
        List<Object> ekNameList = new ArrayList<Object>();
        String result = "F(";
        try {
            ResultSet rs = databaseMetaData.getExportedKeys(null, schemaName, tableName);
            while (rs.next()) {
                String fkColumnName = rs.getString("FKCOLUMN NAME");
                String fkTableName = rs.getString("FKTABLE NAME");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
