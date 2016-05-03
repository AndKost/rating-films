package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by kost on 4/22/16.
 */

//Top films by ages
public class TopFilmsByAge extends RatingJob implements Serializable {

    public TopFilmsByAge(JavaSparkContext context) {
        super(context);
    }

    public Map<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        //Parse u.data text file and get pair (filmId <userID, startRating>) startRating is avgCount class contain rating and number
        //for calculate and compare average rating for each film
        JavaRDD<String> fileData = context.textFile(pathData);

        JavaPairRDD<Integer, Tuple2<Integer, AvgCount>> filmRatingPair = fileData.mapToPair(mapUdataFilmIdIntUserIdRat);

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item text file and get pair (filmId, filmTitle)
        JavaPairRDD<Integer, String> filmIdTitlePair = fileItems.mapToPair(mapUitemFilmIdTitle);

        //Join for change filmId to filmTitle and get pair (filmTitle, <userId, startRating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, AvgCount>>> joinNameFilms = filmIdTitlePair.join(filmRatingPair).values();

        //Make userId is key, get pair(userId <filmTitle, startRating>)
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> userKey = joinNameFilms.mapToPair(convertToIntKeyStrRat);

        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse u.user text file and get pair (userId, ageCategory)
        JavaPairRDD<Integer, String> userAgePair = fileUser.mapToPair(
                (s) -> {
                        String[] row = s.split("\\|");
                        Integer userId = Integer.parseInt(row[0]);
                        Integer age = Integer.parseInt(row[1]);

                        //Split record by group age
                        if (age > 0 && age <= 10)
                            return new Tuple2<>(userId, "0-10");
                        else if (age > 10 && age <= 20)
                            return new Tuple2<>(userId, "10-20");
                        else if (age > 20 && age <= 30)
                            return new Tuple2<>(userId, "20-30");
                        else if (age > 30 && age <= 40)
                            return new Tuple2<>(userId, "30-40");
                        else if (age > 40 && age <= 50)
                            return new Tuple2<>(userId, "40-50");
                        else if (age > 50 && age <= 60)
                            return new Tuple2<>(userId, "50-60");
                        else if (age > 60 && age <= 70)
                            return new Tuple2<>(userId, "60-70");
                        else if (age > 70 && age <= 80)
                            return new Tuple2<>(userId, "70-80");
                        else
                            return new Tuple2<>(userId, "80-90");
                    }
        );

        //Join for change userId to age category and get (AgeCategory <filmTitle startRating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount>>> joinAgeUserKey = userAgePair.join(userKey).values();

        //Make key <age, filmTitle> for calculate average rating, get pair  (<AgeCategory, filmTitleame> startRating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> forCalcRat = joinAgeUserKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating for each film
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts = forCalcRat.reduceByKey(reduceByKeyAvgRating);

        //Make key age and get pair (ageCategory, <filmTitle, averageRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> ageKeyAvgRat = avgCounts.mapToPair(convertToStr1KeyStrRat);

        //Collect films by age category
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByAge = ageKeyAvgRat.groupByKey();

        //Sort and get top 10 films
        JavaPairRDD<String, Iterable<String>> resultAgeFilms = filmsGroupByAge.mapToPair(sortAndTakeByAvgRating);

        return resultAgeFilms.collectAsMap();
    }

}
