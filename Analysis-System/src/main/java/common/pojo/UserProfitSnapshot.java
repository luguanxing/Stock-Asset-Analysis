package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserProfitSnapshot {
    Integer uid;
    Double profit_value;
    Long kakfa_offset;
    Long update_time;

    public UserProfitSnapshot(UserProfit userProfit) {
        this.uid = userProfit.getUid();
        this.profit_value = userProfit.getProfit_value();
        this.kakfa_offset = userProfit.getKafka_offset();
        this.update_time = System.currentTimeMillis();
    }
}
