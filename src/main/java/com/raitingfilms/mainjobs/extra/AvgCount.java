package com.raitingfilms.mainjobs.extra;

import scala.Serializable;

/**
 * Created by kost on 4/23/16.
 */
public class AvgCount implements Serializable {

    public int total;
    public int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public float avg() {
        return total / (float) num;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "total_=" + total +
                ", num_=" + num +
                '}';
    }
};

