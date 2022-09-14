package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserAsset {
    Integer uid;
    Double cashValue;
    Double positionValue;
    Double totalValue;
}
