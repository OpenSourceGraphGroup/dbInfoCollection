package databaseInfoCollector;

import common.Common;
import org.junit.Test;

import java.sql.*;
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
    private Statement st=null;

    @Test
    public void test(){
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        SchemaCollector sc=new SchemaCollector(connection);
        List<Object> tableNameList=sc.getTableList("tpch");
        for(Object table:tableNameList){
//            System.out.print((String)table+":");
//            String fk_info = sc.getFKNameListString(sc.getForeignKeys("tpch",(String)table));
//            if (!fk_info.equals("")) {
//                System.out.println((String)table + ": " + fk_info);
//            }
            System.out.println(sc.getTableInfo("tpch",(String)table));
        }
//        System.out.print(sc.getTableInfo("tpch","customer"));
    }

    public SchemaCollector(Connection conn) {
        try {
            databaseMetaData = (DatabaseMetaData) conn.getMetaData();
            st=conn.createStatement();
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
    public List<Object> getTableList(String dbName) {
        List<Object> tableNameList = new ArrayList<Object>();
        String[] types={"TABLE"};
        try {
            ResultSet rs = databaseMetaData.getTables(dbName, null, "%",types);
            while (rs.next()) {
                tableNameList.add(rs.getString("TABLE_NAME"));
            }
            return tableNameList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 获取表数据量
     * @Description: getTableSize
     * @Param: [schemaName, tableName]
     * @Returns: java.lang.String
     */
    public String getTableSize(String schemaName,String tableName){
        try {
            String sql="select COUNT(*) from "+schemaName+"."+tableName;
            ResultSet rs=st.executeQuery(sql);
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
    public List<Object> getPrimaryKeys(String schemaName, String tableName) {
        List<Object> pkNameList = new ArrayList<Object>();
        try {
            ResultSet rs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);
            while (rs.next()) {
//                String columnName = rs.getString("PK_NAME");//主键名称
//                short keySeq = rs.getShort("KEY SEQ");//序列号
                String pkName = rs.getString("COLUMN_NAME");
                pkNameList.add(pkName);
            }
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
        result=result.substring(0,result.length()-1);
        result+=")";
        return result.toUpperCase();
    }

    /**
     * @Description: getForeignKeys
     * @Param: [schemaName, tableName]
     * @return: java.lang.String -->F(foreign_key, referenced_table.referenced_primary_key)
     * @Author: Jiaye Liu
     * @Date: 9:54
     */
    public List<Object> getForeignKeys(String schemaName, String tableName) {
        List<Object> ekList = new ArrayList<Object>();
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
            return ekList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 外键列表转为String
     * @Description: getFKNameListString
     * @Param: [fkNameList]
     * @Returns: java.lang.String
     */
    public String getFKNameListString(List<Object> fkNameList) {
        String result="";
        for (Object i: fkNameList) {
            result += "F(";
            result+=(ForeignKeys)i;
            result+=");";
        }
        if(result.length()<=0){
            return "";
        }else {
            result = result.substring(0, result.length() - 1);
            return result.toUpperCase();
        }
    }

    /** 获取表的列名与数据类型信息
     * @Description: getTableColumns
     * @Param: [schemaName, tableName]
     * @Returns: java.util.List<java.lang.Object>
     */
    public List<Object> getTableColumns(String schemaName,String tableName){
        List<Object> tableColumns=new ArrayList<Object>();
        try {
            ResultSet rs=databaseMetaData.getColumns(null,schemaName,tableName,"%");
            while(rs.next()){
                String columnName=rs.getString("COLUMN_NAME");
                String columnType=rs.getString("TYPE_NAME");
                if(columnType.toUpperCase().equals("CHAR"))
                    columnType="VARCHAR";
                if(columnType.toUpperCase().equals("INT"))
                    columnType="INTEGER";
                Column c=new Column(schemaName,tableName,columnName,columnType);
                tableColumns.add(c);
            }
            return tableColumns;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 输出表的所有信息
     * @Description: getTableInfo
     * @Param: [schemaName, tableName]
     * @Returns: java.lang.String
     */
    public String getTableInfo(String schemaName,String tableName){
        List<Object> columnList= getTableColumns(schemaName,tableName);
        List<Object> pkList=getPrimaryKeys(schemaName,tableName);
        List<Object> fkList=getForeignKeys(schemaName,tableName);
        String result="T[";
        result+=tableName+";"+getTableSize(schemaName,tableName)+";";
        for(Object c:columnList){
            Column tmp=(Column)c;
            result+=tmp.toString()+";";
        }
        result+=getPKNameListString(pkList)+";";
        result+=getFKNameListString(fkList);
        result=result.substring(0,result.length()-1);
        result+=")]";
        return result.toUpperCase();
    }

}

class ForeignKeys{
    String fkName;
    String fkTable;
    List<Object> pkNameList;
    public ForeignKeys(String fkName,String fkTable){
        this.fkName=fkName;
        this.fkTable=fkTable;
        this.pkNameList=new ArrayList<Object>();
    }
    public void appendPKName(String pkName){
        this.pkNameList.add(pkName);
    }
    @Override
    public String toString() {
        String result="";
        result+=fkName+",";
        for(Object s:pkNameList){
            result+=this.fkTable+".";
            result+=(String)s;
            result+=",";
        }
        result=result.substring(0,result.length()-1);
        return result;
    }
}

