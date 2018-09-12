package com.xian.liankloud.dao;

import com.xian.liankloud.domain.AdProvinceTop3;

import java.util.List;

public interface IAdProvinceTop3Dao
{
    /**
     * 批量更新
     * @param apts
     */
    void updateBatch(List<AdProvinceTop3> apts);
}
