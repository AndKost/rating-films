package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.RatingJob;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by kost on 5/2/16.
 */
public class TopAndDiscussedFilmsByOccupation extends RatingJob {

    private PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<String>> calcResultSortAndTake;

    public TopAndDiscussedFilmsByOccupation(JavaSparkContext context, boolean isMostDiscussed) {
        super(context);
        if (isMostDiscussed)
            this.calcResultSortAndTake = super.sortAndTakeByAvgRating;
        else
            this.calcResultSortAndTake = CountJob.sortAndTakeByCntDis;
    }

    public Map<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.data file and get (filmId, <userId, startRating>)
        JavaPairRDD<Integer, Tuple2<Integer, AvgCount>> filmIdUserIdRatingPair = fileData.mapToPair(mapUdataFilmIdIntUserIdRat);

        JavaRDD<String> fileUItem = context.textFile(pathItem);

        //Parse from uitem text file and get pair (filmId, filmTitle)
        JavaPairRDD<Integer, String> filmTitlePair = fileUItem.mapToPair(mapUitemFilmIdTitle);

        //Join pairs to change filmId to gender and get (title, <userId startRating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, AvgCount>>> joinTitlerKey = filmTitlePair.join(filmIdUserIdRatingPair).values();

        //Make key (userId, <title, rating>)
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> userIdKeyPairs = joinTitlerKey.mapToPair(convertToIntKeyStrRat);

        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse u.user text file and get pairs (userId, occupation)
        JavaPairRDD<Integer, String> userIdGenderPairs = fileUser.mapToPair(mapUuserUserIdOccupation);

        //Join pairs for change userId to occupation and get pairs (occupation <title, rating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount>>> genderKey = userIdGenderPairs.join(userIdKeyPairs).values();

        //Make key (<occupation, title>, rating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> forAvgPairs = genderKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts = forAvgPairs.reduceByKey(reduceByKeyAvgRating);

        //Make key occupation get (occupation, <title, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = avgCounts.mapToPair(convertToStr1KeyStrRat);

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGender = userIdKeyAvgRat.groupByKey();

        //Sort and get top occupation
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByGender.mapToPair(calcResultSortAndTake);

        return result.collectAsMap();
    }
}
