import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
class Common {
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    static Connection connect(String ip, String db, String user, String password) {
        return connect(ip, "3306", db, user, password);
    }

    private static Connection connect(String ip, String port, String db, String user, String password) {
        Connection connection = null;
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC", ip, port, db);
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(url, user, password);
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

    enum WriteType {Override, Append}

    static void log(String content) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        writeTo(df.format(new Date()) + "\t" + content, "out/log.log", WriteType.Append);
    }
    static void info(String content){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        writeTo(df.format(new Date()) + "\t" + content, "out/info.log", WriteType.Append);
    }

    static void error(String content) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        writeTo(df.format(new Date()) + "\t" + content, "out/error.log", WriteType.Append);
    }

    static void writeTo(String content, String filePath) {
        writeTo(content, filePath, WriteType.Override);
    }

    private static void writeTo(String content, String filePath, WriteType writeType) {
        System.out.print(content);
        File file = new File(filePath);
        try {
            Writer out = new FileWriter(file, writeType.equals(WriteType.Append));
            out.write(content);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Given the schema, table and attribute, return if the attribute is a foreign key in that table
     */
    static boolean isFK(Connection connection, String schema, String tableName, String attribute) throws SQLException {
        String sql = String.format("select count(*) from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA='%s' " +
                        "and TABLE_NAME='%s' and CONSTRAINT_NAME != 'PRIMARY' and COLUMN_NAME='%s'", schema,
                tableName, attribute);
        ResultSet resultSet = Common.query(connection, sql);

        if (resultSet.next()) {
            int count = resultSet.getInt(1);
            return count >= 1;
        }
        return false;
    }

    /**
     * Given the schema, table and attribute, return if the attribute is a primary key in that table
     */
    static boolean isPK(Connection connection, String schema, String tableName, String attribute) throws SQLException {
        String sql = String.format("select count(*) from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA='%s' " +
                        "and TABLE_NAME='%s' and CONSTRAINT_NAME = 'PRIMARY' and COLUMN_NAME='%s'", schema,
                tableName, attribute);
        ResultSet resultSet = Common.query(connection, sql);

        if (resultSet.next()) {
            int count = resultSet.getInt(1);
            return count >= 1;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    static <T> T cast(Object obj) {
        return (T) obj;
    }
}
