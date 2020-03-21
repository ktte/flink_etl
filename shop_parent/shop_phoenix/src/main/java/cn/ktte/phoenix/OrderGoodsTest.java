package cn.ktte.phoenix;

import java.sql.*;

/**
 * 使用Phoenix来查询Hbase中的订单明细宽表数据
 */
public class OrderGoodsTest {

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        try {
            //JDBC的使用步骤
            //1. 注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //2. 获取连接
            connection = DriverManager.getConnection("jdbc:phoenix:node1:2181", "", "");
            //3. 创建Statement
            statement = connection.createStatement();
            //4. 编写SQL
            String sql = "select * from \"dwd_order_detail\" limit 10";
            //5. 用Statement来执行SQL查询语句
            ResultSet resultSet = statement.executeQuery(sql);
            //6. 获取查询结果.比如打印一下.
            while (resultSet.next()) {
                //开始获取数据
                String rowid = resultSet.getString("rowid");
                String ogId = resultSet.getString("ogId");
                String orderId = resultSet.getString("orderId");
                String goodsId = resultSet.getString("goodsId");
                String goodsNum = resultSet.getString("goodsNum");
                String goodsPrice = resultSet.getString("goodsPrice");
                String goodsName = resultSet.getString("goodsName");
                String shopId = resultSet.getString("shopId");
                String goodsThirdCatId = resultSet.getString("goodsThirdCatId");
                String goodsThirdCatName = resultSet.getString("goodsThirdCatName");
                String goodsSecondCatId = resultSet.getString("goodsSecondCatId");
                String goodsSecondCatName = resultSet.getString("goodsSecondCatName");
                String goodsFirstCatId = resultSet.getString("goodsFirstCatId");
                String goodsFirstCatName = resultSet.getString("goodsFirstCatName");
                String areaId = resultSet.getString("areaId");
                String shopName = resultSet.getString("shopName");
                String shopCompany = resultSet.getString("shopCompany");
                String cityId = resultSet.getString("cityId");
                String cityName = resultSet.getString("cityName");
                String regionId = resultSet.getString("regionId");
                String regionName = resultSet.getString("regionName");

                System.out.print(rowid + "\t");
                System.out.print(ogId + "\t");
                System.out.print(orderId + "\t");
                System.out.print(goodsId + "\t");
                System.out.print(goodsNum + "\t");
                System.out.print(goodsPrice + "\t");
                System.out.print(goodsName + "\t");
                System.out.print(shopId + "\t");
                System.out.print(goodsThirdCatId + "\t");
                System.out.print(goodsThirdCatName + "\t");
                System.out.print(goodsSecondCatId + "\t");
                System.out.print(goodsSecondCatName + "\t");
                System.out.print(goodsFirstCatId + "\t");
                System.out.print(goodsFirstCatName + "\t");
                System.out.print(areaId + "\t");
                System.out.print(shopName + "\t");
                System.out.print(shopCompany + "\t");
                System.out.print(cityId + "\t");
                System.out.print(cityName + "\t");
                System.out.print(regionId + "\t");
                System.out.print(regionName + "\t");
                System.out.println();
            }
            //7. 关闭连接.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
