package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by kost on 4/17/16.
 */

//Total top10 rating
public class TotalTopFilms extends RatingJob implements Serializable {

    //Override number of films
    private static int TOP_COUNT = 10;

    public TotalTopFilms(JavaSparkContext context) {
        super(context);
    }

    public JavaRDD<Tuple2<String, AvgCount>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.date text file to pair(filmId, startRating), startRating is avgCount class contain sum and number for each film.
        // It done for get and compare average rating
        JavaPairRDD<Integer, AvgCount> filmIdRating = fileData.mapToPair(mapUdataFilmIdAvgRat);

        //Calculate average rating for each film
        JavaPairRDD<Integer, AvgCount> avgCounts = filmIdRating.reduceByKey(reduceByKeyAvgRating);

        JavaRDD<String> fileItem = context.textFile(pathItem);

        //Parse u.item text file and get pair (filmId, filmTitle)
        JavaPairRDD<Integer, String> filmName = fileItem.mapToPair(mapUitemFilmIdTitle);

        //Join for change filmId to title and get pair (filmTitle, average rating for each film)
        JavaRDD<Tuple2<String, AvgCount>> joinPair = filmName.join(avgCounts).values();

        //Swap to sort by average rating, sort and swap to back pairs (filmTitle, avgRating) and take 10 films
        JavaRDD<Tuple2<String, AvgCount>> topFilms = (JavaRDD<Tuple2<String, AvgCount>>) joinPair.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(10);

        return topFilms;
    }

}
