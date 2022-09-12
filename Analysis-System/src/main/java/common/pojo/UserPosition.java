package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserPosition {
    String uid;
    Integer stockId;
    Double quantity;
}
