import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void main(String[] args) {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        String sql = String.format("explain %s", Common.getSql("sql/4.sql"));
        ResultSet resultSet = Common.query(connection, sql);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Map> queryPlan = new ArrayList<>();
            while (resultSet.next()) {
                Map rowData = new HashMap();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                queryPlan.add(rowData);
                System.out.println(rowData);
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
