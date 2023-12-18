package io.sustc.dto;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The authorization information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthInfo implements Serializable {

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;

    public boolean isValid(Connection con){
        String sql=null;
        String sql1="select * from UserRecord where mid="+mid;
        if (mid<=0||(qq==null&&wechat==null))return false;
        if (qq!=null&&wechat!=null){
            sql="select * from UserRecord where qq="+qq+" and wechat="+wechat;
        }else if (qq==null){
            sql="select * from UserRecord where wechat="+wechat;
        }else if (wechat==null){
            sql="select * from UserRecord where qq="+qq;
        }
        try{
            PreparedStatement stmt=con.prepareStatement(sql);
            PreparedStatement stmt1=con.prepareStatement(sql1);
            ResultSet rest=stmt.executeQuery();
            ResultSet rs1=stmt1.executeQuery();
            if (!rs1.next())return false;
            if (rs1.getString("password").equals(password))return true;
            if (!rest.next())return false;
            if (rest.getLong("Mid")!=mid)return false;

            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    public boolean isSuperUser(Connection con){
        String sql="select * from user where mid="+mid;
        try {
            PreparedStatement stmt= con.prepareStatement(sql);
            ResultSet rs= stmt.executeQuery();
            if (rs.next()&&
                    !rs.getString("identity").equals("superuser")){
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
