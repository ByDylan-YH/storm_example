package project.utils;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class MyDbUtils {// 拒绝继承
    private static String password = "admin";
    private static QueryRunner queryRunner = new QueryRunner();

    //    拒绝new一个实例
    private MyDbUtils() {
    }

    static {
        try {
//            调用该类时既注册驱动
            String className = "com.mysql.jdbc.Driver";
            Class.forName(className);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) {
    }

    public static List<String> executeQuerySql(String sql) {
        new BasicRowProcessor();
        List<String> result = new ArrayList<>();
        try {
            List<Object[]> requstList = queryRunner.query(getConnection(), sql,
                    new ArrayListHandler(new BasicRowProcessor()));
//                @Override
//                public <Object> List<Object> toBeanList(ResultSet rs,Class<Object> type) throws SQLException {
//                  return super.toBeanList(rs, type);
//                }
            for (Object[] objects : requstList) {
                result.add(objects[0].toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void update(String sql, Object... params) {
        try {
            Connection connection = getConnection();
            queryRunner.update(connection, sql, params);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //    获取连接
    private static Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://192.168.182.1:3306/data?useUnicode=true&characterEncoding=utf-8";
        String user = "root";
        return DriverManager.getConnection(url, user, password);
    }
}