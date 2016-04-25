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
 * Created by kost on 4/19/16.
 */
public class TopGenreForUsers extends Job implements Serializable {

    public TopGenreForUsers(JavaSparkContext context) {
        super(context);
    }


    public Map<String, Iterable<String>> run(String pathData, String pathItem) {

        //Parse uData file and get (filmId, <userId, rating>)
        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse uData file and get (filmId, <userId, rating>) userId - string for universe saving in mongoDB
        JavaPairRDD<Integer, Tuple2<String, Integer>> filmratingPair = fileData.mapToPair(
                (s) -> {
                        String[] row = s.split("\t");
                        String userId = row[0];
                        Integer filmId = Integer.parseInt(row[1]);
                        Integer rating = Integer.parseInt(row[2]);
                        Tuple2<String, Integer> tmp = new Tuple2<String, Integer>(userId, rating);
                        return new Tuple2<Integer, Tuple2<String, Integer>>(filmId, tmp);
                    }
        );


        //Parse from item idFilm genre get pair (idFilm, genre)
        JavaRDD<String> fileUItem = context.textFile(pathItem);

        JavaPairRDD<Integer, String> filmGenrePair = fileUItem.mapToPair(
                (s) -> {
                        String[] row = s.split("\\|");
                        Integer filmId = Integer.parseInt(row[0]);
                        //Separate by genre
                        if (row[5].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "unknown");
                        }
                        if (row[6].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Action");
                        }
                        if (row[7].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Adventure");
                        }
                        if (row[8].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Animation");
                        }
                        if (row[9].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Children's");
                        }
                        if (row[10].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Comedy");
                        }
                        if (row[11].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Crime");
                        }
                        if (row[12].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Documentary");
                        }
                        if (row[13].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Drama");
                        }
                        if (row[14].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Fantasy");
                        }
                        if (row[15].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Film-Noir");
                        }
                        if (row[16].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Horror");
                        }
                        if (row[17].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Musical");
                        }
                        if (row[18].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Mystery");
                        }
                        if (row[19].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Romance");
                        }
                        if (row[20].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Sci-Fi");
                        }
                        if (row[21].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "Thriller");
                        }
                        if (row[22].equals("1")) {
                            return new Tuple2<Integer, String>(filmId, "War");
                        }

                        return new Tuple2<Integer, String>(filmId, "Western");
                    }
        );



        //Left join replace idFilm to genre get (genre, <userid rating>)
        JavaRDD<Tuple2<String, Tuple2<String, Integer> >> joinGenreKey = filmGenrePair.join(filmratingPair).values();

        //Make key (<genre, userid> rating)
        JavaPairRDD<Tuple2<String, String>, Integer> filmRatingPairs = joinGenreKey.mapToPair(
                (s) -> {
                        String genre = s._1();
                        String userId = s._2()._1;
                        Integer rating = s._2._2;
                        Tuple2<String, String> tmpTuple = new Tuple2<String, String>(userId, genre);
                        return new Tuple2<Tuple2<String, String>, Integer>(tmpTuple, rating);
                    }

        );

        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts =
                filmRatingPairs.combineByKey(createAcc, addAndCount, combine);

        //Make key userId get (userId, <genre, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = avgCounts.mapToPair(
                (s) -> {
                        String userId = s._1()._1;
                        String genre = s._1()._2;
                        AvgCount rating = s._2;
                        Tuple2<String, AvgCount> tmpTuple = new Tuple2<String, AvgCount>(genre, rating);
                        return new Tuple2<String, Tuple2<String, AvgCount>>(userId, tmpTuple);
                    }
        );

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByAge = userIdKeyAvgRat.groupByKey();

        //Sort and get top genre
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByAge.mapToPair(SORT_AND_TAKE);

        return result.collectAsMap();
    }


}
