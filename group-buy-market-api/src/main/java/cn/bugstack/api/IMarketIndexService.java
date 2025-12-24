package cn.bugstack.api;

import cn.bugstack.api.dto.*;
import cn.bugstack.api.response.Response;

import java.util.List;

/**
 * @author Fuzhengwei bugstack.cn @小傅哥
 * @description 营销首页服务接口
 * @create 2025-02-02 16:02
 */
public interface IMarketIndexService {

    /**
     * 查询拼团营销配置
     *
     * @param goodsMarketRequestDTO 营销商品信息
     * @return 营销配置信息
     */
    Response<GoodsMarketResponseDTO> queryGroupBuyMarketConfig(GoodsMarketRequestDTO goodsMarketRequestDTO);

    /**
     * 查询拼团营销排行榜
     *
     * @param goodsMarketRankRequestDTO 营销商品信息
     * @return 营销排行榜信息
     */
    Response<GoodsMarketRankListResponseDTO> queryGroupBuyMarketRankList(GoodsMarketRankRequestDTO goodsMarketRankRequestDTO) throws Exception;
}
