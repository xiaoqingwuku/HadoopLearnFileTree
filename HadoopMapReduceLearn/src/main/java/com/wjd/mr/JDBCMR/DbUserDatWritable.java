package com.wjd.mr.JDBCMR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DbUserDatWritable implements WritableComparable<DbUserDatWritable>, DBWritable {

    private int id;
    private int salary;

    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, id);
        statement.setInt(2, salary);

    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        salary = resultSet.getInt(2);

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(salary);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        salary = in.readInt();
    }

    public int compareTo(DbUserDatWritable o) {
        if (id != o.id){
            return (id < o.id ? -1 : (id == o.id ? 0 : 1));
        }

        return (salary < o.salary ? -1 : (salary == o.salary ? 0 : 1));
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(DbUserDatWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              int thisValue = readInt(b1, s1);
              int thatValue = readInt(b2, s2);
              if(thisValue != thatValue){
                  return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
              }

              thisValue = readInt(b1, s1 + 4);
              thatValue = readInt(b2, s2 + 4);

              return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

    static { // register this comparator
        WritableComparator.define(DbUserDatWritable.class, new Comparator());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + salary;
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
        if (!(obj instanceof DbUserDatWritable)) {
            return false;
        }
        DbUserDatWritable other = (DbUserDatWritable) obj;
        if (id != other.id) {
            return false;
        }
        if (salary != other.salary) {
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

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

}