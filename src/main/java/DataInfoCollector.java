import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/**
 * @program: dbInfoCollection
 * @description: 数据统计信息采集
 * @author: Jiaye Liu
 * @create: 2019-12-03 16:27
 **/
public class DataInfoCollector {
    private Statement st = null;

    public static void main(String arg[]) {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        SchemaCollector sc = new SchemaCollector(connection);
        long tableSize = Long.parseLong(sc.getTableSize("tpch", "lineitem"));
        DataInfoCollector dic = new DataInfoCollector(connection);
        String result = dic.getDataStatistics("tpch", "lineitem", tableSize, sc.getTableColumns("tpch", "lineitem"));
        System.out.print(result);
    }

    public DataInfoCollector(Connection conn) {
        try {
            DatabaseMetaData databaseMetaData = (DatabaseMetaData) conn.getMetaData();
            st = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getDataStatistics(String schemaName, String tableName, long tableSize, List<Object> columns) {
        StringBuilder result = new StringBuilder();
        for (Object c : columns) {
            Column tmp = (Column) c;
            String dataInfo = "";
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
            result.append(dataInfo);
        }
        return result.toString().toUpperCase();
    }

    private String ratioToString(double ratio, int precision) {
        BigDecimal bd = new BigDecimal(String.valueOf(ratio));
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);
        return "" + bd;
    }

    private String getNullRation(long tableSize, Column c) {
        //空值占比
        try {
            ResultSet rs = st.executeQuery(c.getNullSizeSQL());
            rs.next();
            long nullSize = Long.parseLong(rs.getString(1));
            double nullRatio = nullSize / (double) tableSize;
            return ratioToString(nullRatio, 2) + "; ";
        } catch (SQLException e) {
            e.printStackTrace();
            return ";";
        }

    }

    private String getInteger(long tableSize, Column c) {
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

    private String getReal(long tableSize, Column c) {
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

    private String getVarchar(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            //平均长度
            ResultSet rs = st.executeQuery(c.getSumLengthSQL());
            rs.next();
            long sumLength = Long.parseLong(rs.getString(1));
            double avg = sumLength / (double) tableSize;
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

    private String getBool(long tableSize, Column c) {
        String result = "D[" + c.tableName + "." + c.columnName + "; ";
        try {
            //空值占比
            result += getNullRation(tableSize, c);
            ResultSet rs = st.executeQuery(c.getTrueSizeSQL());
            long trueSize = Long.parseLong(rs.getString(1));
            double trueRatio = trueSize / (double) tableSize;
            result += ratioToString(trueRatio, 1) + "]";
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
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
        return "select COUNT(DISTINCT " + columnName + ") from " + schemaName + "." + tableName;
    }

    String getMaxValueSQL() {
        return "select MAX(" + columnName + ") from " + schemaName + "." + tableName;
    }

    String getMinValueSQL() {
        return "select MIN(" + columnName + ") from " + schemaName + "." + tableName;
    }

    String getNullSizeSQL() {
        return "select COUNT(*) from " + schemaName + "." + tableName + " where ISNULL(" + columnName + ")";
    }

    String getMaxLengthSQL() {
        return "select MAX(LENGTH(" + columnName + ")) from " + schemaName + "." + tableName;
    }

    String getSumLengthSQL() {
        return "select SUM(LENGTH(" + columnName + ")) from " + schemaName + "." + tableName;
    }

    String getTrueSizeSQL() {
        return "select COUNT(*) from " + schemaName + "." + tableName + " where " + columnName + "=true";
    }

    @Override
    public String toString() {
        String result = "";
        result += columnName + "," + columnType;
        return result;
    }
}