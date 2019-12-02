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

    public String getTableSize(String schemaName,String tableName){
        return "";
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
//                String columnName = rs.getString("COLUMN NAME");//列名
//                short keySeq = rs.getShort("KEY SEQ");//序列号
                String pkName = rs.getString("PK NAME");//主键名称
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
            ResultSet rs = databaseMetaData.getForeignKeys(null, schemaName, tableName);
            while (rs.next()) {
                String fkColumnName = rs.getString("FKCOLUMN NAME");
                String fkTableName = rs.getString("FKTABLE NAME");
                ForeignKeys ek=new ForeignKeys(fkColumnName,fkTableName);
                ResultSet rsTmp=databaseMetaData.getPrimaryKeys(null,schemaName,tableName);
                while(rsTmp.next()){
                    String pkName= rsTmp.getString("PK NAME");
                    ek.appendPKName(pkName);
                }
                ekList.add(ek);
            }
            return ekList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    public String getFKNameListString(List<Object> fkNameList) {
        String result="";
        for (Object i: fkNameList) {
            result += "F(";
            result+=(ForeignKeys)i;
            result+=");";
        }
        result=result.substring(0,result.length()-1);
        return result.toUpperCase();
    }

    public List<Object> getTableColumns(String schemaName,String tableName){
        List<Object> tableColumns=new ArrayList<Object>();
        try {
            ResultSet rs=databaseMetaData.getColumns(null,schemaName,tableName,"%");
            while(rs.next()){
                String columnName=rs.getString("COLUMN NAME");
                String columnType=rs.getString("SQL DATA TYPE");
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
        return result;
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

class Column{
    String schemaName;
    String tableName;
    String columnName;
    String columnType;
    public Column(String schemaName,String tableName,String columnName,String columnType){
        this.schemaName=schemaName;
        this.tableName=tableName;
        this.columnName=columnName;
        this.columnType=columnType;
    }

    public String getMaxValueSQL(){
        if(columnType=="Integer"){
            return "select max("+columnName+") from "+schemaName+"."+tableName;
        }
        return "";
    }

    @Override
    public String toString() {
        String result="";
        return result;
    }
}