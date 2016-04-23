package com.raitingfilms.mainjobs.extra;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;

/**
 * Created by kost on 4/23/16.
 */
public class AvgCount implements Serializable {

    public AvgCount(int total, int num) {
        total_ = total;
        num_ = num;
    }

    public int total_;

    @Override
    public String toString() {
        return "AvgCount{" +
                "total_=" + total_ +
                ", num_=" + num_ +
                '}';
    }

    public int num_;

    public float avg() {
        return total_ / (float) num_;
    }
};

