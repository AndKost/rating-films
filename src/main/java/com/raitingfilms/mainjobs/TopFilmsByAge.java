package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by kost on 4/22/16.
 */

//Top films by ages
public class TopFilmsByAge extends Job implements Serializable {

    public TopFilmsByAge(JavaSparkContext context) {
        super(context);
    }

    public Map<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        //Parse u.data to (ItemId <UserID rating>)
        JavaRDD<String> fileData = context.textFile(pathData);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmratingPair = fileData.mapToPair(
                (s) -> {
                        String[] row = s.split("\t");
                        Integer userId = Integer.parseInt(row[0]);
                        Integer filmId = Integer.parseInt(row[1]);
                        Integer rating = Integer.parseInt(row[2]);
                        Tuple2<Integer, Integer> tmp = new Tuple2<Integer, Integer>(userId, rating);
                        return new Tuple2<Integer, Tuple2<Integer, Integer>>(filmId, tmp);
                    }
        );

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item and get pair (idFilm, namefilm)
        JavaPairRDD<Integer, String> filmNamePair = fileItems.mapToPair(
                (s) -> {
                        String[] row = s.split("\\|");
                        Integer filmId = Integer.parseInt(row[0]);
                        String nameFilm = row[1];
                        return new Tuple2<Integer, String>(filmId, nameFilm);
                    }
        );

        //Joint and get pair (nameFilm <userId, rating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> joinNameFilms = filmNamePair.join(filmratingPair).values();

        //Make user id is key (userId <nameFilm, rating>)
        JavaPairRDD<Integer, Tuple2<String, Integer>> userKey = joinNameFilms.mapToPair(
                (s) -> {
                        String nameFilm = s._1();
                        Integer rating = s._2()._2;
                        Integer userId = s._2()._1;
                        Tuple2<String, Integer> tmpTuple = new Tuple2<String, Integer>(nameFilm, rating);
                        return new Tuple2<Integer, Tuple2<String, Integer>>(userId, tmpTuple);
                    }
        );



        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse and get pair (userid, ageCategory)
        JavaPairRDD<Integer, String> userAgePair = fileUser.mapToPair(
                (s) -> {
                        String[] row = s.split("\\|");
                        Integer userId = Integer.parseInt(row[0]);
                        Integer age = Integer.parseInt(row[1]);

                        //Разбиваем по возрастным группам
                        if (age > 0 && age <= 10)
                            return new Tuple2<Integer, String>(userId, "0-10");
                        if (age > 10 && age <= 20)
                            return new Tuple2<Integer, String>(userId, "10-20");
                        if (age > 20 && age <= 30)
                            return new Tuple2<Integer, String>(userId, "20-30");
                        if (age > 30 && age <= 40)
                            return new Tuple2<Integer, String>(userId, "30-40");
                        if (age > 40 && age <= 50)
                            return new Tuple2<Integer, String>(userId, "40-50");
                        if (age > 50 && age <= 60)
                            return new Tuple2<Integer, String>(userId, "50-60");
                        if (age > 60 && age <= 70)
                            return new Tuple2<Integer, String>(userId, "60-70");
                        if (age > 70 && age <= 80)
                            return new Tuple2<Integer, String>(userId, "70-80");

                        return new Tuple2<Integer, String>(userId, "80-90");
                    }
        );

        //Join and get (Age <filmName rating>)
        JavaRDD<Tuple2<String, Tuple2<String, Integer>>> joinAgeUserKey = userAgePair.join(userKey).values();


        //Make pair (<Age, filmName> rating) for calculate avg rating
        JavaPairRDD<Tuple2<String, String>, Integer> forCalcRat = joinAgeUserKey.mapToPair(
                (s) -> {
                        String age = s._1();
                        String nameFilm = s._2()._1;
                        Integer rating = s._2._2;
                        Tuple2<String, String> tmpTuple = new Tuple2<String, String>(nameFilm, age);
                        return new Tuple2<Tuple2<String, String>, Integer>(tmpTuple, rating);
                    }
        );


        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts =
                forCalcRat.combineByKey(createAcc, addAndCount, combine);

        //Make key age get (age, <nameFilm, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> ageKeyAvgRat = avgCounts.mapToPair(
                (s) -> {
                        String age = s._1()._2;
                        String nameFilm = s._1()._1;
                        AvgCount rating = s._2;
                        Tuple2<String, AvgCount> tmpTuple = new Tuple2<String, AvgCount>(nameFilm, rating);
                        return new Tuple2<String, Tuple2<String, AvgCount>>(age, tmpTuple);
                    }
        );

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByAge = ageKeyAvgRat.groupByKey();

        //Sort and get top films
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByAge.mapToPair(SORT_AND_TAKE);

        return result.collectAsMap();

        //testsave
        //result.saveAsTextFile("/home/kost/workspace/rating-films/src/main/resources/resultutest2/");
    }


}
