package common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockQuotation {
    String stock_id;
    Double price;
    Timestamp kafka_timestamp;
    Long kafka_partition;
    Long kafka_offset;
}
