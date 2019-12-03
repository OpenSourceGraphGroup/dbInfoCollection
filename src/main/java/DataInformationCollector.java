import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;

/**
 * @program: dbInfoCollection
 * @description: 数据统计信息采集
 * @author: Jiaye Liu
 * @create: 2019-12-03 16:27
 **/
public class DataInformationCollector {
    private DatabaseMetaData databaseMetaData = null;
    private Statement st = null;

    public static void main(String arg[]) {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        SchemaCollector sc = new SchemaCollector(connection);
        long tableSize = Long.parseLong(sc.getTableSize("tpch", "lineitem"));
        DataInformationCollector dic = new DataInformationCollector(connection);
        String result=dic.getDataStatistics("tpch", "lineitem", tableSize, sc.getTableColumns("tpch", "lineitem"));
//        System.out.print(result);
    }

    public DataInformationCollector(Connection conn) {
        try {
            databaseMetaData = (DatabaseMetaData) conn.getMetaData();
            st = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getDataStatistics(String schemaName, String tableName, long tableSize, List<Object> columns) {
        String result = "";
        for (Object c : columns) {
            Column tmp = (Column) c;
            String dataInfo="";
            switch (tmp.columnType.toUpperCase()) {
                case "INTEGER":
                    dataInfo += getInteger(tableSize, tmp) + "\n";
                    break;
                case "REAL":
                    dataInfo += getReal(tableSize, tmp) + "\n";
                    break;
                case "DECIMAL":
                    dataInfo += getReal(tableSize, tmp) + "\n";
                    break;
                case "VARCHAR":
                    dataInfo += getVarchar(tableSize, tmp) + "\n";
                    break;
                case "BOOL":
                    dataInfo += getBool(tableSize, tmp) + "\n";
                    break;
                case "DATETIME":
                    dataInfo += getReal(tableSize, tmp) + "\n";
                    break;
                case "DATE":
                    dataInfo += getReal(tableSize, tmp) + "\n";
            }
//            System.out.print(dataInfo);
            result+=dataInfo;
        }
        return result.toUpperCase();
    }

    public String ratioToString(double ratio, int precision) {
        BigDecimal bd = new BigDecimal(String.valueOf(ratio));
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);
        return "" + bd;
    }

    public String getNullRation(long tableSize, Column c) {
        //空值占比
        try {
            ResultSet rs = st.executeQuery(c.getNullSizeSQL());
            rs.next();
            long nullSize = Long.parseLong(rs.getString(1));
            double nullRatio = nullSize / tableSize;
            return ratioToString(nullRatio, 2) + "; ";
        } catch (SQLException e) {
            e.printStackTrace();
            return ";";
        }

    }

    public String getInteger(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            //cardinality
            ResultSet rs = st.executeQuery(c.getDistinctSizeSQL());
            rs.next();
            long distinctSize = Long.parseLong(rs.getString(1));
//            double cardinality= distinctSize/tableSize;
//            result+=ratioToString(cardinality,0);
            result += distinctSize + "; ";
            //最大值
            rs = st.executeQuery(c.getMinValueSQL());
            rs.next();
            result += rs.getString(1) + "; ";
            //最小值
            rs = st.executeQuery(c.getMaxValueSQL());
            rs.next();
            result += rs.getString(1) + "]";
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
    }

    public String getReal(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            //最大值
            ResultSet rs = st.executeQuery(c.getMinValueSQL());
            rs.next();
            result += rs.getString(1) + "; ";
            //最小值
            rs = st.executeQuery(c.getMaxValueSQL());
            rs.next();
            result += rs.getString(1) + "]";
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
    }

    public String getVarchar(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            //平均长度
            ResultSet rs = st.executeQuery(c.getSumLengthSQL());
            rs.next();
            long sumLength = Long.parseLong(rs.getString(1));
            double avg = sumLength / tableSize;
            result += ratioToString(avg, 1) + "; ";
            //最大长度
            rs = st.executeQuery(c.getMaxLengthSQL());
            rs.next();
            result += rs.getString(1) + "]";
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    public String getBool(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            ResultSet rs = st.executeQuery(c.getTrueSizeSQL());
            long trueSize = Long.parseLong(rs.getString(1));
            double trueRatio = trueSize / tableSize;
            result += ratioToString(trueRatio, 1) + "]";
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

}

class Column {
    String schemaName;
    String tableName;
    String columnName;
    String columnType;

    public Column(String schemaName, String tableName, String columnName, String columnType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public String getDistinctSizeSQL() {
        return "select COUNT(DISTINCT " + columnName + ") from " + schemaName + "." + tableName;
    }

    public String getMaxValueSQL() {
        return "select MAX(" + columnName + ") from " + schemaName + "." + tableName;
    }

    public String getMinValueSQL() {
        return "select MIN(" + columnName + ") from " + schemaName + "." + tableName;
    }

    public String getNullSizeSQL() {
        return "select COUNT(*) from " + schemaName + "." + tableName + " where ISNULL(" + columnName + ")";
    }

    public String getMaxLengthSQL() {
        return "select MAX(LENGTH(" + columnName + ")) from " + schemaName + "." + tableName;
    }

    public String getSumLengthSQL() {
        return "select SUM(LENGTH(" + columnName + ")) from " + schemaName + "." + tableName;
    }

    public String getTrueSizeSQL() {
        return "select COUNT(*) from " + schemaName + "." + tableName + " where " + columnName + "=true";
    }

    @Override
    public String toString() {
        String result = "";
        result += columnName + "," + columnType;
        return result;
    }
}