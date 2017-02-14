package com.wjd.mr.JDBCMR;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DbUserWritable implements Writable, DBWritable {
    private int id;
    private String name;
   // DBWritable的方法，设置操作statement
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, id);
        statement.setString(2, name);

    }
   // DBWritable的方法，把数据库记录转换成自定义对象
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DbUserWritable)) {
            return false;
        }
        DbUserWritable other = (DbUserWritable) obj;
        if (id != other.id) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}