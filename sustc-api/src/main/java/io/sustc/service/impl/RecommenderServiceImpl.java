package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RecommenderServiceImpl implements RecommenderService {
    private DataSource dataSource;
    @Override
    public List<String> recommendNextVideo(String bv) {
         String sql1="select * from videos where bv="+bv;
         List<String> list=new ArrayList<>();

         try(Connection conn= dataSource.getConnection();
             PreparedStatement stmt1=conn.prepareStatement(sql1);
         ) {
             ResultSet rs1=stmt1.executeQuery();
             if (!rs1.next())return null;//查不到视频
             String sql="select bv ,count(*)cnt from view where\n" +
                     "    mid in\n" +
                     "              (select mid\n" +
                     "                from view\n" +
                     "                where bv = '"+bv+"')\n" +
                     "and bv<>'"+bv+"'\n" +
                     "group by bv\n" +
                     "order by cnt desc\n" +
                     "limit 5";
             PreparedStatement stmt=conn.prepareStatement(sql);
             ResultSet rs=stmt.executeQuery();
             while (rs.next()){
                 String topbv=rs.getString("bv");
                 list.add(topbv);

             }
             return list;






         } catch (SQLException e) {
             throw new RuntimeException(e);
         }

    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
          if (pageNum<=0||pageSize<=0)return null;
          List<String> list=new ArrayList<>();
          String sql="select bv,case when watchcnt=0 then 0\n" +
                  "else cast(likecnt as numeric)/(watchcnt+likenotwatchcnt)\n" +
                  "    +cast(coincnt as numeric)/(watchcnt+coinnotwatchcnt)\n" +
                  "    +cast(favcnt as numeric)/(watchcnt+favnotwatchcnt)\n" +
                  "    +cast(danmucnt as numeric)/(watchcnt)\n" +
                  "    +cast(sumwatchtime as numeric)/(watchcnt*duration)\n" +
                  "     end score\n" +
                  "from (select bv,duration,\n" +
                  "             (select count(*) from view where view.bv = videos.bv)     watchcnt,\n" +
                  "             (select count(*) from like_ where like_.bv = videos.bv)   likecnt,\n" +
                  "             (select count(*) from like_ where like_.bv = videos.bv and not exists(select * from view where view.bv = videos.bv and like_.mid = view.mid)) likenotwatchcnt,\n" +
                  "             (select count(*) from coin where coin.bv=videos.bv) coincnt,\n" +
                  "             (select count(*) from coin where coin.bv=videos.bv and not exists(select * from view where view.bv = videos.bv and coin.mid = view.mid))coinnotwatchcnt,\n" +
                  "             (select count(*) from favorite where favorite.bv=videos.bv) favcnt,\n" +
                  "             (select count(*) from favorite where favorite.bv=videos.bv and not exists(select * from view where view.bv = videos.bv and favorite.mid = view.mid))favnotwatchcnt,\n" +
                  "             (select count(*) from danmu where danmu.bv=videos.bv) danmucnt,\n" +
                  "             (select sum(watchTime) from view where view.bv=videos.bv) sumwatchtime\n" +
                  "      from videos)tem\n" +
                  "order by score desc\n" +
                  "limit "+pageSize+" offset "+(pageNum-1)*pageSize;
          try(Connection conn=dataSource.getConnection();
              PreparedStatement stmt=conn.prepareStatement(sql);
               ) {
               ResultSet rs=stmt.executeQuery();
               while (rs.next()){
                   String bv=rs.getString("bv");
                   list.add(bv);
               }
               return list;

          } catch (SQLException e) {
              throw new RuntimeException(e);
          }

    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
               String sql="select view.bv,count(*) cnt,(select publicTime from videos where videos.bv=view.bv) publictime,\n" +
                       "       (select Level from userrecord where userrecord.mid=(select ownerMid from videos where videos.bv=view.bv))level\n" +
                       "from view\n" +
                       "where view.mid in (\n" +
                       "select user_mid from followings\n" +
                       "where user_mid in\n" +
                       "      (select following_mid\n" +
                       "               from followings\n" +
                       "               where user_mid = '"+auth.getMid()+"'\n" +
                       "               )\n" +
                       "and following_mid='"+auth.getMid()+"')\n" +
                       "and bv not in (select bv from view\n" +
                       "where mid='"+auth.getMid()+"')\n" +
                       "group by view.bv\n" +
                       "order by cnt desc ,publictime desc ,level desc\n" +
                       "limit "+pageSize+" offset "+(pageNum-1)*pageSize;

             try (
                     Connection conn= dataSource.getConnection();
                     PreparedStatement stmt=conn.prepareStatement(sql);
                     ){
                 if (!auth.isValid(conn)||pageSize<=0||pageNum<=0)return null;
                 List<String> list=new ArrayList<>();
                 ResultSet rs=stmt.executeQuery();
                 while (rs.next()){
                     String bv=rs.getString("bv");
                     list.add(bv);

                 }
                 return list;






             } catch (SQLException e) {
                 throw new RuntimeException(e);
             }

    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
         String sql="select user_mid ,count(*) cnt,(select level from userrecord where userrecord.mid=followings.user_mid)level\n" +
                 "from followings\n" +
                 "where following_mid in (\n" +
                 "    select following_mid from Followings\n" +
                 "where user_mid='"+auth.getMid()+"'\n" +
                 "    )\n" +
                 "and user_mid not in (\n" +
                 "    select following_mid from Followings\n" +
                 "where user_mid='"+auth.getMid()+"'\n" +
                 "    )\n" +
                 "and user_mid<>'"+auth.getMid()+"'\n" +
                 "group by user_mid\n" +
                 "order by cnt desc ,level desc \n" +
                 "limit "+pageSize+" offset "+(pageNum-1)*pageSize;

        try (
                Connection conn= dataSource.getConnection();
                PreparedStatement stmt=conn.prepareStatement(sql);
        ){
            if (!auth.isValid(conn)||pageSize<=0||pageNum<=0)return null;
            List<Long> list=new ArrayList<>();
            ResultSet rs=stmt.executeQuery();
            while (rs.next()){
                long mid=rs.getLong("user_mid");
                list.add(mid);

            }
            return list;






        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
