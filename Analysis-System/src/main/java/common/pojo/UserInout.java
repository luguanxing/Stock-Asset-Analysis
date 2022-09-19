package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInout {
    Integer id;
    Integer uid;
    String inout_type;
    Double inout_value;
    Timestamp kafka_timestamp;
    Long kafka_partition;
    Long kafka_offset;
}
