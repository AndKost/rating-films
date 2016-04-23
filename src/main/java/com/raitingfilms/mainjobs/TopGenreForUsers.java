package com.raitingfilms.mainjobs;

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.*;

/**
 * Created by kost on 4/19/16.
 */
public class TopGenreForUsers {

    private static JavaSparkContext context;

    public TopGenreForUsers(JavaSparkContext context) {
        this.context = context;
    }


    public static final PairFunction<Tuple2<Tuple2<Integer, Integer>, Optional<String>>,    Tuple2<Integer, String>, Integer> KEY_VALUE_PAIRER =
            new PairFunction<Tuple2<Tuple2<Integer, Integer>, Optional<String>>, Tuple2<Integer, String>, Integer>() {
                public Tuple2<Tuple2<Integer, String>, Integer> call(Tuple2<Tuple2<Integer, Integer>, Optional<String>> a) throws Exception {

                    Integer rating = a._1()._2;
                    Tuple2<Integer, String> t = new Tuple2<Integer, String>(a._1()._1, a._2().get());


                    return new Tuple2<Tuple2<Integer, String>, Integer>(t, rating);
                }


            };


    public static final PairFunction<Tuple2<Tuple2<Integer, String>, Integer>,  Integer, Tuple2<String, Integer>> KEY_VALUE_PAIRER2 =
            new PairFunction<Tuple2<Tuple2<Integer, String>, Integer>, Integer, Tuple2<String, Integer>>() {
                public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<Tuple2<Integer, String>, Integer> a) throws Exception {

                    Integer userid = a._1()._1;

                    Tuple2<String, Integer> t = new Tuple2<String, Integer>(a._1()._2, a._2());


                    return new Tuple2<Integer, Tuple2<String, Integer>>(userid, t);
                }


            };


    public static final PairFunction<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>,  Integer, Iterable<Tuple2<String, Integer>>> KEY_VALUE_PAIRER3 =
            new PairFunction<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>, Integer, Iterable<Tuple2<String, Integer>>>() {
                public Tuple2<Integer, Iterable<Tuple2<String, Integer>>> call(Tuple2<Integer, Iterable<Tuple2<String, Integer>>> a) throws Exception {

                    Integer userId = a._1();


                    //Сортируем и выбираем 5 максимальных по рейтингу
                    Iterable<Tuple2<String, Integer>> it = (Iterable<Tuple2<String, Integer>>) a._2();

                    List<Tuple2<String, Integer>> lst = new ArrayList<Tuple2<String, Integer>>();
                    Iterator<Tuple2<String, Integer>> iter = it.iterator();

                    while (iter.hasNext()) {
                        lst.add(iter.next());
                    }

                    //Сортируем второй элемент
                    Collections.sort(lst, new Comparator<Tuple2<String, Integer>>() {
                        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                            return o2._2 - o1._2;
                        }
                    });
                    //Проверка если есть 3 жанра то 3 если меньше то ничего не делать
                    if (lst.size() > 6)
                    lst= lst.subList(0,5);

                    Iterable<Tuple2<String, Integer>> l = lst;
                    //Tuple2<String, Integer> t = new Tuple2<String, Integer>(a._1()._2, a._2());


                    return new Tuple2<Integer, Iterable<Tuple2<String, Integer>>>(userId, l);
                }


            };


    //Считываем из data idUser idFilm rating
    public static void run(String uData, String uItem) {
        JavaRDD<String> fileData = context.textFile(uData);

        //Parse u.date text file to pair(idFilm, pair(userId, rating)
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> filmratingPair = fileData.mapToPair(
                new PairFunction<String, Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<Integer, Integer>> call(String s) throws Exception {
                        String[] row = s.split("	");
                        Integer userId = Integer.parseInt(row[0]);
                        Integer filmId = Integer.parseInt(row[1]);
                        Integer rating = Integer.parseInt(row[2]);
                        Tuple2<Integer, Integer> tmp = new Tuple2<Integer, Integer>(userId, rating);
                        return new Tuple2<Integer, Tuple2<Integer, Integer>>(filmId, tmp);
                    }
                }
        );


        //Parse from item idFilm genre get pair (idFilm, genre)

        JavaRDD<String> fileUItem = context.textFile(uItem);

        JavaPairRDD<Integer, String> filmGenrePair = fileUItem.mapToPair(
                new PairFunction<String, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(String s) throws Exception {
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
                }
        );



        //Left join replace idFilm to genre
        JavaRDD<Tuple2<Tuple2<Integer, Integer>, Optional<String>>> leftjoinOutput = filmratingPair.leftOuterJoin(filmGenrePair).values().distinct();

        //Модифицируем результат в javaPairRRD
        //JavaPairRDD<Tuple2<Integer, Integer>, String> filmRatingPairs = leftjoinOutput.mapToPair(KEY_VALUE_PAIRER);
        JavaPairRDD<Tuple2<Integer, String>, Integer> filmRatingPairs = leftjoinOutput.mapToPair(KEY_VALUE_PAIRER);

        //Считаем суммы по рейтингам
        JavaPairRDD<Tuple2<Integer, String>, Integer> filmRatingCounts = filmRatingPairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );



        //cgroup берем для каждого ключа его жанры по каждому
        //JavaPairRDD<Tuple2<Integer, String>, Iterable<Integer>> filmRatingPairsGroupBykey = filmRatingCounts.groupByKey();

        //Переделыввем пару в pair (userId pair(string rating))
        JavaPairRDD<Integer, Iterable<Tuple2<String, Integer>>> filmRatingPairsOther = filmRatingCounts.mapToPair(KEY_VALUE_PAIRER2).groupByKey();


        //Пытаемся оставить топ 5 записей для каждлго ид
        JavaPairRDD<Integer, Iterable<Tuple2<String, Integer>>> tmptmp = filmRatingPairsOther.mapToPair(KEY_VALUE_PAIRER3);


        tmptmp.saveAsTextFile("/home/kost/workspace/rating-films/src/main/resources/resultutest2/");
    }


}
