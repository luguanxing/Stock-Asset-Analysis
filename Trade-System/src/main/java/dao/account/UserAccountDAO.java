package dao.account;

import utils.PropertiesUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UserAccountDAO {

    private static Connection conn;
    private static PreparedStatement ps;
    private static ResultSet rs;
    private static String jdbcUrl;
    private static String database;
    private static String username;
    private static String password;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            jdbcUrl = String.format(
                    "jdbc:mysql://%s:%s/",
                    PropertiesUtil.getProperties().get("input.db.host"),
                    PropertiesUtil.getProperties().get("input.db.port")
            );
            database = PropertiesUtil.getProperties().get("input.db.database");
            username = PropertiesUtil.getProperties().get("input.db.username");
            password = PropertiesUtil.getProperties().get("input.db.password");
            conn = DriverManager.getConnection(jdbcUrl + database, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addInout(Integer uid, String inoutType, Double inoutValue) throws Exception {
        String insertSql = "insert tb_user_inout (uid, inout_type, inout_value) values (?,?,?)";
        ps = conn.prepareStatement(insertSql);
        ps.setInt(1, uid);
        ps.setString(2, inoutType);
        ps.setDouble(3, inoutValue);
        System.out.println(" done. return = =" + ps.executeUpdate());
    }

    public static void updateInout(Integer inoutId, String inoutType, Double inoutValue) throws Exception {
        String updateSql = "update tb_user_inout set inout_type=?, inout_value=? where id=?";
        ps = conn.prepareStatement(updateSql);
        ps.setString(1, inoutType);
        ps.setDouble(2, inoutValue);
        ps.setInt(3, inoutId);
        System.out.println(" done. return = =" + ps.executeUpdate());
    }

    public static void buyStock(Integer uid, String stockId, Double buyQuantity) throws Exception {
        Double price = null;
        Double cashValue = 0.0;
        Double positionQuantity = 0.0;

        // query price
        String queryPriceSql = "select price from tb_stock_quotation where stock_id=?";
        ps = conn.prepareStatement(queryPriceSql);
        ps.setString(1, stockId);
        rs = ps.executeQuery();
        while (rs.next()) {
            price = rs.getDouble(1);
        }
        if (price == null) {
            System.out.println("no price");
            return;
        }

        // query cash
        String queryCashSql = "select cash_value from tb_user_cash where uid=?";
        ps = conn.prepareStatement(queryCashSql);
        ps.setInt(1, uid);
        rs = ps.executeQuery();
        while (rs.next()) {
            cashValue = rs.getDouble(1);
        }

        // query position quantity
        String queryPositionSql = "select quantity from tb_user_position where uid=? and stock_id=?";
        ps = conn.prepareStatement(queryPositionSql);
        ps.setInt(1, uid);
        ps.setString(2, stockId);
        rs = ps.executeQuery();
        while (rs.next()) {
            positionQuantity = rs.getDouble(1);
        }

        // update cashValue&position in one transaction
        conn.setAutoCommit(false);
        // support margin buy
        double newCashValue = cashValue - buyQuantity * price;
        double newPositionQuantity = positionQuantity + buyQuantity;
        String updateCashSql = "update tb_user_cash set cash_value=? where uid=?";
        ps = conn.prepareStatement(updateCashSql);
        ps.setDouble(1, newCashValue);
        ps.setInt(2, uid);
        ps.executeUpdate();
        String updatePositionSql = "update tb_user_position set quantity=? where uid=? and stock_id=?";
        ps = conn.prepareStatement(updatePositionSql);
        ps.setDouble(1, newPositionQuantity);
        ps.setInt(2, uid);
        ps.setString(3, stockId);
        ps.executeUpdate();

        // commit transaction
        conn.commit();
        System.out.println("done.");
    }

    public static void queryUserAsset(Integer uid) throws Exception {
        Double cash = null;
        Double position = null;
        Double asset = null;
        String querySql = "select cash_value, position_value, total_value from user_asset where uid=?";
        ps = conn.prepareStatement(querySql);
        ps.setInt(1, uid);
        rs = ps.executeQuery();
        while (rs.next()) {
            cash = rs.getDouble(1);
            position = rs.getDouble(2);
            asset = rs.getDouble(3);
        }
        System.out.println(String.format("uid=%d -> [cash=%f, position=%f, asset=%f]", uid, cash, position, asset));
    }

    public static void queryUserProfit(Integer uid) throws Exception {
        Double profit = null;
        String querySql = "select profit_value from user_profit where uid=?";
        ps = conn.prepareStatement(querySql);
        ps.setInt(1, uid);
        rs = ps.executeQuery();
        while (rs.next()) {
            profit = rs.getDouble(1);
        }
        System.out.println(String.format("uid=%d -> [profit=%f]", uid, profit));
    }

    public static void queryUserAssetSnapshot(Integer uid, Long startTs, Long endTs) throws Exception {
        List<List<Double>> snapshot = new ArrayList<>();
        String querySql = "select ts, asset from user_asset_snapshot where uid=? and ? <= ts and ts <= ?";
        ps = conn.prepareStatement(querySql);
        ps.setInt(1, uid);
        ps.setLong(2, startTs);
        ps.setLong(3, endTs);
        rs = ps.executeQuery();
        while (rs.next()) {
            long ts = rs.getLong(1);
            double asset = rs.getDouble(2);
            snapshot.add(Arrays.asList(ts * 1.0, asset));
        }
        System.out.println("uid=" + uid);
        System.out.println(snapshot);
    }

    public static void queryUserProfitSnapshot(Integer uid, Long startTs, Long endTs) throws Exception {
        List<List<Double>> snapshot = new ArrayList<>();
        String querySql = "select ts, profit from user_profit_snapshot where uid=? and ? <= ts and ts <= ?";
        ps = conn.prepareStatement(querySql);
        ps.setInt(1, uid);
        ps.setLong(2, startTs);
        ps.setLong(3, endTs);
        rs = ps.executeQuery();
        while (rs.next()) {
            long ts = rs.getLong(1);
            double asset = rs.getDouble(2);
            snapshot.add(Arrays.asList(ts * 1.0, asset));
        }
        System.out.println("uid=" + uid);
        System.out.println(snapshot);
    }

    public static void main(String[] args) throws Exception {
        // addInout(999, "test", 999.999);
        // updateInout(6, "test111", 888.88);
        // buyStock(1, "00700.HK", 3.0);
        // buyStock(1, "00700.HK", -1.0);
        // queryUserAsset(1);
        // queryUserProfit(1);
        // queryUserAssetSnapshot(2, Long.MIN_VALUE, Long.MAX_VALUE);
        // queryUserProfitSnapshot(1, Long.MIN_VALUE, Long.MAX_VALUE);
    }

}
