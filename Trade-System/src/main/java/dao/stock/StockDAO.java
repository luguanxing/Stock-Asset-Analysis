package dao.stock;

import utils.PropertiesUtil;

import java.sql.*;

public class StockDAO {
    private static Connection conn;
    private static PreparedStatement ps;
    private static ResultSet rs;

    public static void upsertStockPrice(String stockId, Double price) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/",
                PropertiesUtil.getProperties().get("input.db.host"),
                PropertiesUtil.getProperties().get("input.db.port")
        );
        String database = PropertiesUtil.getProperties().get("input.db.database");
        String username = PropertiesUtil.getProperties().get("input.db.username");
        String password = PropertiesUtil.getProperties().get("input.db.password");
        try {
            boolean isExist = false;
            conn = DriverManager.getConnection(jdbcUrl + database, username, password);
            String checkSql = "select * from tb_stock_quotation where stock_id=?";
            ps = conn.prepareStatement(checkSql);
            ps.setString(1, stockId);
            rs = ps.executeQuery();
            while (rs.next()) {
                isExist = true;
            }
            if (!isExist) {
                String insertSql = "insert tb_stock_quotation (stock_id,price) values (?,?)";
                ps = conn.prepareStatement(insertSql);
                ps.setString(1, stockId);
                ps.setDouble(2, price);
            } else {
                String updateSql = "update tb_stock_quotation set price=? where stock_id=?";
                ps = conn.prepareStatement(updateSql);
                ps.setDouble(1, price);
                ps.setString(2, stockId);
            }
            System.out.println("执行完成，返回值=" + ps.executeUpdate());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //4、关闭资源
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        upsertStockPrice("00700.HK", 300.0);
        upsertStockPrice("00600.HK", 123.456);
    }

}
