package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class VideoServiceImpl implements VideoService {
    private DataSource dataSource;

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        return null;
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        Connection conn=null;
        try {
           conn = dataSource.getConnection();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (!auth.isValid(conn))return null;
        if (keywords==null||keywords.equals(""))return null;
        if (pageNum<=0||pageSize<=0)return null;

         String[]keywordarr=keywords.split(" ");
         StringBuilder sb=new StringBuilder();
         sb.append("select bv,(select (");
        for (int i = 0; i < keywordarr.length; i++) {
            sb.append("(length(title)-length(regexp_replace(title,'"+keywordarr[i]+"','','gi')))/length('"+keywordarr[i]+"')+");
        }
        for (int i = 0; i < keywordarr.length; i++) {
            sb.append("(coalesce(length(description),0)-coalesce(length(regexp_replace(description,'"+keywordarr[i]+"','','gi')),0))/length('"+keywordarr[i]+"')+");
        }
        for (int i = 0; i < keywordarr.length; i++) {
            if (i== keywordarr.length-1){
                sb.append("(length(ownerName)-length(regexp_replace(ownerName,'"+keywordarr[i]+"','','gi')))/length('"+keywordarr[i]+"')))relevance,");
            }else {
                sb.append("(length(ownerName)-length(regexp_replace(ownerName,'"+keywordarr[i]+"','','gi')))/length('"+keywordarr[i]+"')+");
            }
        }
        sb.append("(select count(*) from view where view.bv=videos.bv) viewcount from videos where title ~* '");
        for (int i = 0; i < keywordarr.length; i++) {
            if (i== keywordarr.length-1){
                sb.append(keywordarr[i]);
            }else {
                sb.append(keywordarr[i]+"|");
            }
        }
        sb.append("' or description ~* '");
        for (int i = 0; i < keywordarr.length; i++) {
            if (i== keywordarr.length-1){
                sb.append(keywordarr[i]);
            }else {
                sb.append(keywordarr[i]+"|");
            }
        }
        sb.append("' or ownerName ~* '");
        for (int i = 0; i < keywordarr.length; i++) {
            if (i== keywordarr.length-1){
                sb.append(keywordarr[i]);
            }else {
                sb.append(keywordarr[i]+"|");
            }
        }
        sb.append("' order by relevance,viewcount limit "+pageSize+" offset "+(pageNum-1)*pageSize);
        String sql1=sb.toString();
        List<String> list=new ArrayList<>();
        try (
                PreparedStatement stmt1=conn.prepareStatement(sql1);){
            ResultSet rs1=stmt1.executeQuery();
            while (rs1.next()){
                String bv=rs1.getString("bv");
                String sql="select * from videos where bv="+bv;
                PreparedStatement stmt=conn.prepareStatement(sql);
                ResultSet rs=stmt.executeQuery();
                rs.next();
                Timestamp reviewtime=rs.getTimestamp("reviewTime");
                Timestamp publictime=rs.getTimestamp("publicTime");
                long ownermid=rs.getLong("ownerMid");
                Date date=new Date();
                Timestamp currenttime=new Timestamp(date.getTime());
                if ((reviewtime==null||publictime==null||currenttime.before(publictime))&&!(auth.isSuperUser(conn)||auth.getMid()==ownermid)){

                }else {
                    list.add(bv);
                }

            }
            return list;



        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        Set<Integer> set=new HashSet<>();
        String sql1="select * from videos where bv="+bv;
        String sql2="select * from danmu where bv="+bv;

        try(
                Connection conn= dataSource.getConnection();
                PreparedStatement stmt1=conn.prepareStatement(sql1);
                PreparedStatement stmt2=conn.prepareStatement(sql2);
                ){
            ResultSet rs1=stmt1.executeQuery();//找不到bv
            if (!rs1.next())return set;
            ResultSet rs2=stmt2.executeQuery();//视频没弹幕
            if (!rs2.next())return set;
            String sql="select floor(time/10) index,count(*)\n" +
                    "from danmu where bv='"+bv+"'\n" +
                    "group by index\n" +
                    "having count(*)=(select max(cnt)\n" +
                    "from (\n" +
                    "    select floor(time/10) index,count(*) cnt\n" +
                    "from danmu where bv='1'\n" +
                    "group by index\n" +
                    "     )tem)";
            PreparedStatement stmt=conn.prepareStatement(sql);
            ResultSet rs=stmt.executeQuery();
            while (rs.next()){
                int index=rs.getInt("index");
                set.add(index);
            }
            return set;






        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
