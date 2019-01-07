package com.imooc.dao;

import com.imooc.domain.VideoAccessTopN;
import com.imooc.utils.MySQLUtils;
import com.mysql.jdbc.MySQLConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VideoAccessTopNDAO {
    static Map<String, String> courses = new HashMap<String, String>();
    static {
        courses.put("4000", "MySQL优化");
        courses.put("4500", "Crontab");
        courses.put("4600", "Swift");
        courses.put("14540", "SpringData");
        courses.put("14704", "R");
        courses.put("14390", "机器学习");
        courses.put("14322", "redis");
        courses.put("14390", "神经网络");
        courses.put("14623", "Docker");
    }
    /*
    *  查询当天最受欢迎的top5课程
    * */
    public List<VideoAccessTopN> query(String day){
        List<VideoAccessTopN> list = new ArrayList<VideoAccessTopN>();

        Connection connection = null;
        PreparedStatement psmt = null;
        ResultSet rs = null;

        try{
            connection = MySQLUtils.getConnection();
            String sql = "select cms_id, times from day_video_access_topn_stat where day = ? order by times desc limit 5";

            psmt = connection.prepareStatement(sql);
            psmt.setString(1, day);
            rs = psmt.executeQuery();

            VideoAccessTopN domain = null;
            while(rs.next()){
                domain =new VideoAccessTopN();
                domain.setName(rs.getLong("cms_id")+"");
                domain.setValue(rs.getLong("times"));
                list.add(domain);
            }

        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            MySQLUtils.release(connection, psmt, rs);
        }
        return list;
    }


    public static void main(String[] args){
        VideoAccessTopNDAO dao = new VideoAccessTopNDAO();
        List<VideoAccessTopN> list = dao.query("20170511");
        for(VideoAccessTopN result: list) {
            System.out.println(courses.get(result.getName()) + "," + result.getValue());
        }
    }
}
