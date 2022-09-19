package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserProfit {
    Integer uid;
    Double profit_value;
    Timestamp kafka_timestamp;
    Long kafka_partition;
    Long kafka_offset;
    String version;

    public UserProfit(Integer uid, Double profit_value, String version) {
        this.uid = uid;
        this.profit_value = profit_value;
        this.version = version;
    }
}
