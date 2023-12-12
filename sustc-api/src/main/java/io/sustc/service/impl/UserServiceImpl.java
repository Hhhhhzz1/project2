package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UserServiceImpl implements UserService {
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        String sql1="select max(Mid) from UserRecord";
        String sql2="select * from UserRecord where qq="+req.getQq()+" or wechat="+req.getWechat()+" or name="+req.getName();
        String sql3="insert into UserRecord(Mid,Name,Sex,Birthday,Sign,password,qq,wechat) values(?,?,?,?,?,?,?,?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(sql1);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3)
        ) {
            ResultSet rs1 = stmt1.executeQuery();
            rs1.next();
            ResultSet rs2=stmt2.executeQuery();
            long registeredMid=rs1.getLong(1)+1;
            if (req.getPassword()==null||req.getName()==null||req.getSex()==null)return -1;
            if ((req.getBirthday()!=null||!req.getBirthday().equals(""))&&(!req.getBirthday().matches("1[12]月1?")))return -1;
            if (rs2.next())return -1;
            String sex;
            if (req.getSex()== RegisterUserReq.Gender.MALE){
                sex="Male";
            }else if (req.getSex()== RegisterUserReq.Gender.FEMALE){
                sex="Female";
            }else {
                sex="Unknown";
            }
            stmt3.setLong(1,registeredMid);
            stmt3.setString(2,req.getName());
            stmt3.setString(3,sex);
            stmt3.setString(4, req.getBirthday());
            stmt3.setString(5,req.getSign());
            stmt3.setString(6,req.getPassword());
            stmt3.setString(7,req.getQq());
            stmt3.setString(8,req.getWechat());
            stmt3.executeUpdate();
            return registeredMid;



        } catch (SQLException e) {
            e.printStackTrace();
            return -1;

        }



    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        String sql1="select Mid from UserRecord where qq="+auth.getQq();
        String sql2="select Mid from UserRecord where wechat="+auth.getWechat();
        String sql3="delete from UserRecord where Mid="+mid;

        if (mid<=0) return false;//检查mid合法
        if ((auth.getQq()==null||auth.getQq().equals(""))||(auth.getWechat()==null||auth.getWechat().equals("")))return false;
//检查auth里的qq微信非空
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(sql1);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3)

        ) {
            ResultSet rs1 = stmt1.executeQuery();
            ResultSet rs2=stmt2.executeQuery();

            if (!rs1.next()||!rs2.next())return false;//通过qq或微信找不到这个人

            int mid1=rs1.getInt(1);//通过qq找到的人
            int mid2=rs2.getInt(1);//通过微信找到的人
            if (mid1!=mid2)return false;
            if(auth.getMid()!=mid)return false;
            if (mid1!=mid) return false;//通过qq微信找到的人相同，但和要删除的人不同
            stmt3.executeUpdate();
            return true;


        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }





    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        String sql1="select * from UserRecord where qq="+auth.getQq();
        String sql2="select * from UserRecord where wechat="+auth.getWechat();
        String sql3="select * from UserRecord where Mid="+followeeMid;
        String sql4="insert into Followings(user_mid,following_mid) values(?,?)";
        if ((auth.getQq()==null||auth.getQq().equals(""))||(auth.getWechat()==null||auth.getWechat().equals("")))return false;
//检查auth里的qq微信非空
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(sql1);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3);
             PreparedStatement stmt4 = conn.prepareStatement(sql4);

        ) {
            ResultSet rs1 = stmt1.executeQuery();
            ResultSet rs2=stmt2.executeQuery();

            if (!rs1.next()||!rs2.next())return false;//通过qq或微信找不到这个人

            int mid1=rs1.getInt(1);//通过qq找到的人
            int mid2=rs2.getInt(1);//通过微信找到的人
            if (mid1!=mid2)return false;
            if (mid1!=auth.getMid()) return false;//通过qq微信找到的人相同，但和要auth的人不同
            if (followeeMid<=0)return false;
            ResultSet rs3=stmt3.executeQuery();

            if (!rs3.next())return false;//找不到followeemid对应的人

            stmt4.setLong(1,auth.getMid());
            stmt4.setLong(2,followeeMid);
            stmt4.executeUpdate();
            return true;





        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }







    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        String sql1="select * from UserRecord where Mid="+mid;
        String sql2="select coin from UserRecord where Mid="+mid;
        String sql3="select following_mid from Followings where user_mid="+mid;
        String sql4="select count(*) from Followings where user_mid="+mid;
        String sql5="select user_mid from Followings where following_mid="+mid;
        String sql6="select count(*) from Followings where following_mid="+mid;
        String sql7="select count(*) from ViewRecord where user_mid="+mid;
        String sql8="select BV from ViewRecord where user_mid="+mid;
        String sql9="select count(*) from Likes where user_mid="+mid;
        String sql10="select BV from Likes where user_mid="+mid;
        String sql11="select count(*) from Favorite where user_mid="+mid;
        String sql12="select BV from Favorite where user_mid="+mid;
        String sql13="select count(*) from VideoRecord where OwnerMid="+mid;
        String sql14="select BV from VideoRecord where OwnerMid="+mid;
        if (mid<=0)return null;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(sql1);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3);
             PreparedStatement stmt4 = conn.prepareStatement(sql4);
             PreparedStatement stmt5 = conn.prepareStatement(sql5);
             PreparedStatement stmt6 = conn.prepareStatement(sql6);
             PreparedStatement stmt7 = conn.prepareStatement(sql7);
             PreparedStatement stmt8 = conn.prepareStatement(sql8);
             PreparedStatement stmt9 = conn.prepareStatement(sql9);
             PreparedStatement stmt10 = conn.prepareStatement(sql10);
             PreparedStatement stmt11 = conn.prepareStatement(sql11);
             PreparedStatement stmt12 = conn.prepareStatement(sql12);
             PreparedStatement stmt13 = conn.prepareStatement(sql13);
             PreparedStatement stmt14 = conn.prepareStatement(sql14)
        ){
            ResultSet rs1=stmt1.executeQuery();

            if (!rs1.next())return null;//查不到mid对应的人

            UserInfoResp userInfoResp=new UserInfoResp();
            userInfoResp.setMid(mid);//mid

            ResultSet rs2=stmt2.executeQuery();
            rs2.next();
            userInfoResp.setCoin(rs2.getInt(1));//coin

            ResultSet rs4=stmt4.executeQuery();
            rs4.next();
            int followingCount=rs4.getInt(1);
            long[] followingMids=new long[followingCount];
            int k1=0;
            ResultSet rs3=stmt3.executeQuery();
            while (rs3.next()){
                followingMids[k1]=rs3.getLong(1);
                k1++;
            }
            userInfoResp.setFollowing(followingMids);//following



            ResultSet rs6=stmt6.executeQuery();
            rs6.next();
            int followerCount=rs6.getInt(1);
            long[] followerMids=new long[followerCount];
            int k2=0;
            ResultSet rs5=stmt5.executeQuery();
            while (rs5.next()){
                followerMids[k2]=rs5.getLong(1);
                k2++;
            }
            userInfoResp.setFollower(followerMids);//follower

            ResultSet rs7=stmt7.executeQuery();
            rs7.next();
            int watchCount=rs7.getInt(1);
            String[] watchedBVs=new String[watchCount];
            int k3=0;
            ResultSet rs8=stmt8.executeQuery();
            while (rs8.next()){
                watchedBVs[k3]=rs8.getString(1);
                k3++;
            }
            userInfoResp.setWatched(watchedBVs);//watched


            ResultSet rs9=stmt9.executeQuery();
            rs9.next();
            int likeCount=rs9.getInt(1);
            String[] likeBVs=new String[likeCount];
            int k4=0;
            ResultSet rs10=stmt10.executeQuery();
            while (rs10.next()){
                likeBVs[k4]=rs10.getString(1);
                k4++;
            }
            userInfoResp.setLiked(likeBVs);//liked

            ResultSet rs11=stmt11.executeQuery();
            rs11.next();
            int collectCount=rs11.getInt(1);
            String[] collectBVs=new String[collectCount];
            int k5=0;
            ResultSet rs12=stmt12.executeQuery();
            while (rs12.next()){
                collectBVs[k5]=rs12.getString(1);
                k5++;
            }
            userInfoResp.setCollected(collectBVs);//collected

            ResultSet rs13=stmt13.executeQuery();
            rs13.next();
            int postCount=rs13.getInt(1);
            String[] postBVs=new String[postCount];
            int k6=0;
            ResultSet rs14=stmt14.executeQuery();
            while (rs14.next()){
                postBVs[k6]=rs14.getString(1);
                k6++;
            }
            userInfoResp.setPosted(postBVs);//posted
            return userInfoResp;





        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }

    }
}
