package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by kost on 5/8/16.
 */
public class TopGenresByDiscussedByOccupation extends CountJob {


    public TopGenresByDiscussedByOccupation(JavaSparkContext context) {
        super(context);
    }

    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.data file and get (filmId, <userId, startRating>)
        JavaPairRDD<Integer, Tuple2<Integer, AvgCount>> filmIdUserIdRatingPair = fileData.mapToPair(mapUdataFilmIdIntUserIdRat);

        JavaRDD<String> fileUItem = context.textFile(pathItem);

        //Parse from uitem text file and get pair (filmId, genre)
        JavaPairRDD<Integer, String> filmTitlePair = fileUItem.flatMapToPair(generateFilmIdGenrePairs);

        //Join pairs to change filmId to genre and get (genre, <userId startRating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, AvgCount>>> joinTitlerKey = filmTitlePair.join(filmIdUserIdRatingPair).values();

        //Make key (userId, <genre, rating>)
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> userIdKeyPairs = joinTitlerKey.mapToPair(convertToIntKeyStrRat);

        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse u.user text file and get pairs (userId, occupation)
        JavaPairRDD<Integer, String> userIdGenderPairs = fileUser.mapToPair(mapUuserUserIdOccupation);

        //Join pairs for change userId to occupation and get pairs (occupation <genre, rating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount>>> genderKey = userIdGenderPairs.join(userIdKeyPairs).values();

        //Make key (<occupation, genre>, rating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> forAvgPairs = genderKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts = forAvgPairs.reduceByKey(reduceByKeyAvgRating);

        //Make key occupation get (occupation, <genre, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = avgCounts.mapToPair(convertToStr1KeyStrRat);

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGender = userIdKeyAvgRat.groupByKey();

        //Sort and get top occupation
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByGender.mapToPair(CountJob.sortAndTakeByCntDis);

        return result;
    }
}
