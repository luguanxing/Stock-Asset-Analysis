package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserAssetSnapshot {
    Integer uid;
    Double total_value;
    Long kakfa_offset;
    Long update_time;

    public UserAssetSnapshot(UserAsset userAsset) {
        this.uid = userAsset.getUid();
        this.total_value = userAsset.getTotal_value();
        this.kakfa_offset = userAsset.getKafka_offset();
        this.update_time = System.currentTimeMillis();
    }
}
