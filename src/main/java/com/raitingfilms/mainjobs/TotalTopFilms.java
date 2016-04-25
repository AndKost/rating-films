package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Created by kost on 4/17/16.
 */

//Total top10 rating
public class TotalTopFilms extends Job implements Serializable {

    //override variable
    private int TOP_COUNT = 10;

    public TotalTopFilms(JavaSparkContext context) {
        super(context);
    }

    public List<Tuple2<String, AvgCount>> run(String pathData, String pathItem) {

        JavaRDD<String> file = context.textFile(pathData);

        //Parse u.date text file to pair(id, rating)
        JavaPairRDD<Integer, Integer> filmRating = file.mapToPair(
                new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String s) throws Exception {
                        String[] row = s.split("\t");
                        Integer filmId = Integer.parseInt(row[1]);
                        Integer rating = Integer.parseInt(row[2]);
                        return new Tuple2<Integer, Integer>(filmId, rating);
                    }
                }
        );

        //Calculate Average for each film
        JavaPairRDD<Integer, AvgCount> avgCounts =
                filmRating.combineByKey(createAcc, addAndCount, combine);

        JavaRDD<String> fileFilms = context.textFile(pathItem);

        //Parse u.item and get pair (id, namefilm)
        JavaPairRDD<Integer, String> filmInfo = fileFilms.mapToPair(
                new PairFunction<String, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String s) throws Exception {
                        String[] row = s.split("\\|");
                        Integer filmId = Integer.parseInt(row[0]);
                        String name = row[1];
                        return new Tuple2<Integer, String>(filmId, name);
                    }
                }
        );


        //Join and get pair (filmName, AvgCount) AvgCount contain average
        JavaRDD<Tuple2<String, AvgCount>> joinPair = filmInfo.join(avgCounts).values();


        //Sort by rating and take 10 films
        List<Tuple2<String, AvgCount>> topFilms = joinPair.takeOrdered(
                TOP_COUNT,
                new CountComparator()
        );

        return topFilms;
    }

    private class CountComparator implements Comparator<Tuple2<String, AvgCount>>, Serializable {
        @Override
        public int compare(Tuple2<String, AvgCount> o1, Tuple2<String, AvgCount> o2){

            if (o2._2().avg() < o1._2().avg()) return -1;
            if (o2._2().avg() > o1._2().avg()) return 1;
            return 0;
        }
    }
}
