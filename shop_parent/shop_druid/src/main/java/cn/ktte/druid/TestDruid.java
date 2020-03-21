package cn.ktte.druid;

import java.sql.*;
import java.util.Properties;

/**
 * DruidJDBC连接测试
 */
public class TestDruid {
    public static void main(String[] args) {

        try {
            // 1. 加载Druid JDBC驱动
            Class.forName("org.apache.calcite.avatica.remote.Driver");
            // 2. 获取Druid JDBC连接
            Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://node3:8888/druid/v2/sql/avatica/",
                    new Properties());

            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery("SELECT city, sum(\"count\") filter(where city != 'beijing') AS totalNum FROM \"ad_event\" GROUP BY city");

            while (resultSet.next()) {
                String city = resultSet.getString("city");
                String totalNum = resultSet.getString("totalNum");
                System.out.println("city: " + city + " totalNum: " + totalNum);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
        }


    }
}
