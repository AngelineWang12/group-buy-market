package cn.bugstack.api.dto;

import cn.bugstack.api.dto.GoodsMarketRankResponseDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GoodsMarketRankListResponseDTO {
    private Long activityId;
    private String rankType;
    private String timeWindow;
    private String windowKey;
    private Date updateTime;
    private List<GoodsMarketRankResponseDTO> rankList;
}
