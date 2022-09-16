package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserPosition {
    Integer uid;
    String stock_id;
    Double quantity;
    Timestamp kafka_timestamp;
    Long kafka_partition;
    Long kafka_offset;
}
