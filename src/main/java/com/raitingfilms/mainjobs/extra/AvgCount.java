package com.raitingfilms.mainjobs.extra;

import scala.Serializable;

import java.util.Comparator;

/**
 * Created by kost on 4/23/16.
 */
public class AvgCount implements Serializable, Comparable<AvgCount> {

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

    @Override
    public int compareTo(AvgCount nextVal) {
        if (nextVal.avg() < this.avg()) return -1;
        if (nextVal.avg() > this.avg()) return 1;
        return 0;
    }

};

