package com.raitingfilms;

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Created by kost on 4/17/16.
 */

//Total top10 rating
public class TotalTopFilms {

    private static JavaSparkContext context;

    public TotalTopFilms(JavaSparkContext context) {
        this.context = context;
    }

    public static List<Tuple2<String, Integer>> run(String fpath, String dpath) {
        JavaRDD<String> file = context.textFile(fpath);

        //Parse u.date text file to pair(id, rating)
        JavaPairRDD<Integer, Integer> filmratingPair = file.mapToPair(
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


        JavaRDD<String> fileFilms = context.textFile(dpath);

        //Parst u.item and get pair (id, namefilm)
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


        //Соединяем сеты leftjoin
        JavaRDD<Tuple2<String, Optional<Integer>>> leftjoinOutput = filmInfo.leftOuterJoin(filmCounts).values().distinct();
        //Получили сет "Название фильма" его рейтинг


        //Модифицируем результат в javaPairRRD
        JavaPairRDD<String, Integer> filmRatingPairs = leftjoinOutput.mapToPair(KEY_VALUE_PAIRER);



        //Сортируем по рейтигу и выводим топ 10 самых популярных
        List<Tuple2<String, Integer>> top10Films =  filmRatingPairs.takeOrdered(
                10,
                new CountComparator()
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
