package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.Job;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by kost on 4/21/16.
 */
public class TopFilmsByGender extends Job implements Serializable {

    private static JavaSparkContext context;

    public TopFilmsByGender(JavaSparkContext context) {
        super(context);
    }



    public void run(String fpathData, String fpathItem, String fpathGender) {



    }
}
