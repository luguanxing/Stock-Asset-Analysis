package dao.account;

public class UserAccountDAO {

    public static void addUser(Integer uid) throws Exception {

    }

    public static int addUserInout(Integer uid, String inoutType, Double inoutValue) throws Exception {

        return 0;
    }

    public static void updateUserInout(Integer inoutId, String inoutType, Double inoutValue) throws Exception {

    }

    public static void updateUserStockTrade(Integer uid, String stockId, Double quantity) throws Exception {
        // buy or sell
    }

    public static void queryUserAsset(Integer uid) throws Exception {

    }

    public static void queryUserAProfit(Integer uid) throws Exception {

    }

    public static void queryUserAssetSnapshot(Integer uid, Long startTs, Long endTs) throws Exception {

    }

    public static void queryUserProfitSnapshot(Integer uid, Long startTs, Long endTs) throws Exception {

    }

}
