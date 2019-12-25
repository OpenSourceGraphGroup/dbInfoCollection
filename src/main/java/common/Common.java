package common;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: Xin Jin, Zhengmin Lai
 * @Description: Common operation including connect to mysql, get sql statement from file and so on.
 */
public class Common {
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    /**
     * Connect to mysql database using default port 3306
     * @param ip
     * @param db
     * @param user
     * @param password
     * @return
     */
    public static Connection connect(String ip, String db, String user, String password) {
        return connect(ip, "3306", db, user, password);
    }

    /**
     * Connect to mysql database
     * @param ip
     * @param port
     * @param db
     * @param user
     * @param password
     * @return
     */
    public static Connection connect(String ip, String port, String db, String user, String password) {
        Connection connection = null;
        String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true", ip, port, db);
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * Execute sql statement
     * @param connection
     * @param sql
     * @return
     */
    public static ResultSet query(Connection connection, String sql) {
        ResultSet resultSet = null;
        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    /**
     * Read sql statement in sqlPath
     * @param sqlPath
     * @return
     */
    public static String getSql(String sqlPath) {
        File file = new File(sqlPath);
        long fileLength = file.length();
        byte[] content = new byte[(int) fileLength];
        try (FileInputStream in = new FileInputStream(file)) {
            if (in.read(content) != fileLength) System.out.println("Error in reading sql file.");
            return new String(content, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Override: Write from the beginning
     * Append: Write from the end
     */
    public enum WriteType {Override, Append}

    /**
     * log information into out/info.log
     * @param content
     */
    public static void info(String content) {
        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");
        writeTo(df.format(new Date()) + "\t" + content, "log.log", WriteType.Append);
    }

    /**
     * Write error messages into out/error.log
     * @param content
     */
    public static void error(String content) {
        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");
        writeTo(df.format(new Date()) + "\t" + content, "error.log", WriteType.Append);
    }

    /**
     * Write content into file from the beginning in 'out' directory
     * @param content
     * @param filePath
     */
    public static void writeTo(String content, String filePath) {
        writeTo(content, filePath, WriteType.Override);
    }

    /**
     * Write content into file in 'out' directory
     * @param content
     * @param fileName
     * @param writeType WriteType: Override or Append
     */
    public static void writeTo(String content, String fileName, WriteType writeType) {
        File f = new File("out/");
        if (!f.exists()) {
            if (!f.mkdirs()) {
                System.out.println("Create out directory Failed.");
                return;
            }
        }
        fileName = "out/" + fileName;

        System.out.print(content);
        File file = new File(fileName);
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
     * @param connection
     * @param schema
     * @param tableName
     * @param attribute
     * @return
     * @throws SQLException
     */
    public static boolean isFK(Connection connection, String schema, String tableName, String attribute) throws SQLException {
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
     * @param connection
     * @param schema
     * @param tableName
     * @param attribute
     * @return
     * @throws SQLException
     */
    public static boolean isPK(Connection connection, String schema, String tableName, String attribute) throws SQLException {
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

    /**
     * Cast Object obj into need type
     * @param obj
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj) {
        return (T) obj;
    }
}
