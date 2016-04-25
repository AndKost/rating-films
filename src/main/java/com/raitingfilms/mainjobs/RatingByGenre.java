package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by kost on 4/17/16.
 */
public class RatingByGenre extends Job implements Serializable {

    public RatingByGenre(JavaSparkContext context) {
        super(context);
    }

    public Map<String, Iterable<String>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.date text file to pair(idFilm, rating)
        JavaPairRDD<Integer, Integer> filmRating = fileData.mapToPair(
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

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item and get pair (id, <namefilm, genre>)
        JavaPairRDD<Integer, Tuple2<String, String>> filmGenrePair = fileItems.mapToPair(
                new PairFunction<String, Integer, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<String, String>> call(String s) throws Exception {
                        String[] row = s.split("\\|");
                        Integer filmId = Integer.parseInt(row[0]);
                        String nameFilm = row[1];

                        String genre = "unknown";

                        //Each film have one genre
                        //Separate by genre
                        if (row[6].equals("1")) {
                            genre = "Action";
                        }
                        if (row[7].equals("1")) {
                            genre = "Adventure";
                        }
                        if (row[8].equals("1")) {
                            genre = "Animation";
                        }
                        if (row[9].equals("1")) {
                            genre = "Children's";
                        }
                        if (row[10].equals("1")) {
                            genre = "Comedy";
                        }
                        if (row[11].equals("1")) {
                            genre = "Crime";
                        }
                        if (row[12].equals("1")) {
                            genre = "Documentary";
                        }
                        if (row[13].equals("1")) {
                            genre = "Drama";
                        }
                        if (row[14].equals("1")) {
                            genre = "Fantasy";
                        }
                        if (row[15].equals("1")) {
                            genre = "Film-Noir";
                        }
                        if (row[16].equals("1")) {
                            genre = "Horror";
                        }
                        if (row[17].equals("1")) {
                            genre = "Musical";
                        }
                        if (row[18].equals("1")) {
                            genre = "Mystery";
                        }
                        if (row[19].equals("1")) {
                            genre = "Romance";
                        }
                        if (row[20].equals("1")) {
                            genre = "Sci-Fi";
                        }
                        if (row[21].equals("1")) {
                            genre = "Thriller";
                        }
                        if (row[22].equals("1")) {
                            genre = "War";
                        }

                        Tuple2<String, String> outTuple = new Tuple2<String, String>(nameFilm, genre);

                        return new Tuple2<Integer, Tuple2<String, String>>(filmId, outTuple);
                    }
                }
        );

        //Join pairs and get (<nameFilm, genre>, avgrating)
        JavaRDD<Tuple2<Tuple2<String, String>, AvgCount>> joinPair = filmGenrePair.join(avgCounts).values();

        //Make genre is key
        JavaPairRDD<String, Tuple2<String, AvgCount>> genreKey = joinPair.mapToPair(
            new PairFunction<Tuple2<Tuple2<String,String>,AvgCount>, String, Tuple2<String, AvgCount>>() {
                @Override
                public Tuple2<String, Tuple2<String, AvgCount>> call(Tuple2<Tuple2<String, String>, AvgCount> s) throws Exception {
                        String genre = s._1()._2;
                        AvgCount avg = s._2;
                        String nameFilm = s._1._1;
                        Tuple2<String, AvgCount> tmpTuple = new Tuple2<String, AvgCount>(nameFilm, avg);
                    return new Tuple2<String, Tuple2<String, AvgCount>>(genre, tmpTuple);

                }
            }
        );

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGenre = genreKey.groupByKey();

        //Sort and get top films
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByGenre.mapToPair(SORT_AND_TAKE);

        return result.collectAsMap();

        //result.saveAsTextFile("/home/kost/workspace/rating-films/src/main/resources/resultutest2/");

    }



}
