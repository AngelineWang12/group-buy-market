package cn.bugstack.domain.activity.model.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RankItemEntity {
    private String member; // goodsId
    private Long score;    // saleCount
}
