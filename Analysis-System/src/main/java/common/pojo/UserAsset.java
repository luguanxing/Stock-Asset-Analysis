package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserAsset {
    Integer uid;
    Double cash_value;
    Double position_value;
    Double total_value;
    Timestamp kafka_timestamp;
    Long kafka_partition;
    Long kafka_offset;
    String version;

    public UserAsset(Integer uid, Double cash_value, Double position_value, Double total_value, String version) {
        this.uid = uid;
        this.cash_value = cash_value;
        this.position_value = position_value;
        this.total_value = total_value;
        this.version = version;
    }
}
