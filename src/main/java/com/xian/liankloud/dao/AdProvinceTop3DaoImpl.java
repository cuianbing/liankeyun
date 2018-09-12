package com.xian.liankloud.dao;


import com.xian.liankloud.domain.AdProvinceTop3;
import com.xian.liankloud.util.DBCPUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AdProvinceTop3DaoImpl implements IAdProvinceTop3Dao
{

    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void updateBatch(List<AdProvinceTop3> apts)
    {
        try {
        String selectSQL = "SELECT click_count as count FROM user_action_count " +
                "WHERE category = ? " +
                "AND brand = ? " +
                "AND click_count = ?";
        List<AdProvinceTop3> updateList = new ArrayList<>();
        List<AdProvinceTop3> insertList = new ArrayList<>();
        for (AdProvinceTop3 act : apts) {
            Integer clickCount = qr.query(selectSQL, new ScalarHandler<Integer>(), act.getCategory(),act.getBrand(),act.getClick_count());
            if (clickCount != null) {//update
                updateList.add(act);
            } else {//insert
                insertList.add(act);
            }
        }
            //update
            String updateSQL = "UPDATE user_action_count SET click_count=? " +
                    "WHERE category = ? " +
                    "AND brand = ? " +
                    "AND click_count = ?";
            Object[][] updateParams = new Object[updateList.size()][];
            for (int i = 0; i < updateList.size(); i++) {
                AdProvinceTop3 act = updateList.get(i);
                updateParams[i] = new Object[]{act.getCategory(),act.getBrand(),act.getClick_count()};
            }
            qr.batch(updateSQL, updateParams);
            //insert
        String insertSQL="INSERT INTO user_action_count (category,brand,click_count) VALUES(?, ?, ?)";
        Object[][] insertParams = new Object[insertList.size()][];
            for (int i = 0; i < insertList.size(); i++) {
                AdProvinceTop3 act = insertList.get(i);
                insertParams[i] = new Object[]{act.getCategory(),act.getBrand(),act.getClick_count()};
            }
            qr.insertBatch(insertSQL, new BeanListHandler<AdProvinceTop3>(AdProvinceTop3.class), insertParams);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
