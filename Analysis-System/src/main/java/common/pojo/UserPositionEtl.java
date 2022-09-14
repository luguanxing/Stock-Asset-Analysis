package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserPositionEtl {
    Integer uid;
    String stockId;
    Double quantity;
    Double price;
    Double position_value;
}
