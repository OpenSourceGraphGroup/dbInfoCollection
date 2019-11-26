import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;

/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
public class Common {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    static Connection connect(String ip, String db, String user, String password) {
        return connect(ip, "3306", db, user, password);
    }

    static Connection connect(String ip, String port, String db, String user, String password) {
        Connection connection = null;
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC", ip, port, db);
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(url, user, password);
            System.out.println(String.format("Connect to %s in %s", db, ip));
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    static ResultSet query(Connection connection, String sql) {
        ResultSet resultSet = null;
        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    static String getSql(String sqlPath) {
        File file = new File(sqlPath);
        Long fileLength = file.length();
        byte[] content = new byte[fileLength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(content);
            return new String(content, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    static void writeTo(String content, String filePath) {
        File file = new File(filePath);
        try {
            Writer out = new FileWriter(file);
            out.write(content);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T cast(Object obj) {
        return (T) obj;
    }
}
