package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.RatingJob;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.io.Serializable;


/**
 * Created by kost on 4/21/16.
 */
public class TopAndDiscussedFilmsByGender extends RatingJob implements Serializable {

    //Function change work: get top rating film or most discussed films
    private PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<String>> calcResultSortAndTake;

    public TopAndDiscussedFilmsByGender(JavaSparkContext context, boolean isMostDiscussed) {
        super(context);
        if (isMostDiscussed)
            this.calcResultSortAndTake = super.sortAndTakeByAvgRating;
        else
            this.calcResultSortAndTake = CountJob.sortAndTakeByCntDis;
    }

       public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse uData file and get (filmId, <userId, startRating>)
        JavaPairRDD<Integer, Tuple2<Integer, AvgCount>> filmIdUserIdRatingPair = fileData.mapToPair(mapUdataFilmIdIntUserIdRat);

        JavaRDD<String> fileUItem = context.textFile(pathItem);

        //Parse from uitem text file and get pair (filmId, filmTitle)
        JavaPairRDD<Integer, String> filmTitlePair = fileUItem.mapToPair(mapUitemFilmIdTitle);

        //Join pairs to change filmId to gender and get (title, <userId startRating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, AvgCount>>> joinTitlerKey = filmTitlePair.join(filmIdUserIdRatingPair).values();

        //Make key userId and get pairs (userId, <title, rating>)
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> userIdKeyPairs = joinTitlerKey.mapToPair(convertToIntKeyStrRat);

        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse u.user text file and get pairs (userId, gender)
        JavaPairRDD<Integer, String> userIdGenderPairs = fileUser.mapToPair(mapUuserUserIdGender);

        //Join pairs for change userId to gender and get pairs (gender <title, rating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount>>> genderKey = userIdGenderPairs.join(userIdKeyPairs).values();

        //Make key <gender, title> to calculate average rating and get (<gender, title>, rating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> forAvgPairs = genderKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts = forAvgPairs.reduceByKey(reduceByKeyAvgRating);

        //Make key gender get (gender, <title, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = avgCounts.mapToPair(convertToStr1KeyStrRat);

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGender = userIdKeyAvgRat.groupByKey();

        //Sort and get top genre
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByGender.mapToPair(calcResultSortAndTake);

        return result;
    }
}
