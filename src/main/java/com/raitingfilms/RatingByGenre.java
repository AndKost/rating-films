package com.raitingfilms;

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by kost on 4/17/16.
 */
public class RatingByGenre {
    private static JavaSparkContext context;

    public RatingByGenre(JavaSparkContext context) {
        this.context = context;
    }

    static enum Genres { unknown, Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy,
        FilmNoir, Horror, Musical, Mystery, Romance, SciFi, Thriller, War, Western }

    public static List<Tuple2<String, Integer>> run(String fpath, String dpath, String genre) {
        JavaRDD<String> fileData = context.textFile(fpath);

//        Integer t = RatingByGenre.Genres.valueOf(genre);





        //Парсим все ид и рейтинг
        //Parse u.date text file to pair(id, rating)
        JavaPairRDD<Integer, Integer> filmratingPair = fileData.mapToPair(
                new PairFunction<String, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String s) throws Exception {
                        String[] row = s.split("	");

                            Integer filmId = Integer.parseInt(row[1]);
                            Integer rating = Integer.parseInt(row[2]);
                            return new Tuple2<Integer, Integer>(filmId, rating);

                    }
                }
        );

        //Count rating for ech film, pair(id, totalrating)
        JavaPairRDD<Integer, Integer> filmCounts = filmratingPair.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );



        //Парсим те фильмы только с нужным жанром

        JavaRDD<String> fileFilms = context.textFile(dpath);

        //Parst u.item and get pair (id, namefilm)
        //Лютый костыль переписать
        //Сначала маппим все а потмо осталвяем только с нужным жанром через другой сет и мап
        JavaPairRDD<Integer, String> filmInfo = fileFilms.mapToPair(
                new PairFunction<String, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String s) throws Exception {
                        String[] row = s.split("\\|");
                        if (row[7].equals("1")) {
                            Integer filmId = Integer.parseInt(row[0]);
                            String name = row[1];

                            return new Tuple2<Integer, String>(filmId, name);
                        }
                        else return new Tuple2<Integer, String>(0, "0");
                    }
                }
        );



        //Соединяем сеты leftjoin
        JavaRDD<Tuple2<String, Optional<Integer>>> leftjoinOutput = filmInfo.leftOuterJoin(filmCounts).values().distinct();
        //Получили сет "Название фильма" его рейтинг


        //Модифицируем результат в javaPairRRD
        JavaPairRDD<String, Integer> filmRatingPairs = leftjoinOutput.mapToPair(KEY_VALUE_PAIRER);

        //Тесты
       // filmRatingPairs.saveAsTextFile("/home/kost/workspace/rating-films/src/main/resources/resultutest2/");

        //Сортируем по рейтигу и выводим топ 10 самых популярных
        List<Tuple2<String, Integer>> top10Films =  filmRatingPairs.takeOrdered(
                10,
                new com.raitingfilms.TotalTopFilms.CountComparator()
        );

        return top10Films;

    }


    public static class CountComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2){
            return o2._2()- o1._2();
        }
    }

    //Convert javaRRD to javaPairRRD
    public static final PairFunction<Tuple2<String, Optional<Integer>>, String, Integer> KEY_VALUE_PAIRER =
            new PairFunction<Tuple2<String, Optional<Integer>>, String, Integer>() {
                public Tuple2<String, Integer> call(
                        Tuple2<String, Optional<Integer>> a) throws Exception {
                    // a._2.isPresent()
                    return new Tuple2<String, Integer>(a._1, a._2.get());
                }
            };




}
