package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by kost on 4/24/16.
 */
public class Job implements Serializable {

    //Static because else runtimeexeption
    protected static JavaSparkContext context;
    protected int TOP_COUNT = 5;

    public Job(JavaSparkContext context) {
        this.context = context;
    }


    //Methods for count average
    Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
        public AvgCount call(Integer x) {
            return new AvgCount(x, 1);
        }
    };
    Function2<AvgCount, Integer, AvgCount> addAndCount =
            new Function2<AvgCount, Integer, AvgCount>() {
                public AvgCount call(AvgCount a, Integer x) {
                    a.total_ += x;
                    a.num_ += 1;
                    return a;
                }
            };
    Function2<AvgCount, AvgCount, AvgCount> combine =
            new Function2<AvgCount, AvgCount, AvgCount>() {
                public AvgCount call(AvgCount a, AvgCount b) {
                    a.total_ += b.total_;
                    a.num_ += b.num_;
                    return a;
                }
            };

    //Sort and take in group by genre for RatingByGenre and TopFilmByAge classes
    protected PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<String>> SORT_AND_TAKE =
            new PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>, String, Iterable<String>>() {
                public Tuple2<String, Iterable<String>> call(Tuple2<String, Iterable<Tuple2<String, AvgCount>>> a) throws Exception {

                    String genre = a._1();


                    //Sort and take top
                    Iterable<Tuple2<String, AvgCount>> it = (Iterable<Tuple2<String, AvgCount>>) a._2();

                    List<Tuple2<String, AvgCount>> lst = new ArrayList<Tuple2<String, AvgCount>>();
                    Iterator<Tuple2<String, AvgCount>> iter = it.iterator();

                    while (iter.hasNext()) {
                        lst.add(iter.next());
                    }


                    //Sort by average
                    Collections.sort(lst, new Comparator<Tuple2<String, AvgCount>>() {
                        public int compare(Tuple2<String, AvgCount> o1, Tuple2<String, AvgCount> o2) {
                            if (o2._2().avg() < o1._2().avg()) return -1;
                            if (o2._2().avg() > o1._2().avg()) return 1;
                            return 0;
                        }
                    });

                    //Check
                    if (lst.size() >= TOP_COUNT)
                        lst= lst.subList(0,TOP_COUNT);

                    List<String> result = new LinkedList<String>();
                    for (Tuple2<String, AvgCount> t : lst){
                        result.add(t._1);
                    }

                    Iterable<String> l = result;

                    return new Tuple2<String, Iterable<String>>(genre, result);
                }


            };
    //Sort and take with values
    protected PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<Tuple2<String, AvgCount>>> SORT_AND_TAKE_AVG =
            new PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>, String, Iterable<Tuple2<String, AvgCount>>>() {
                public Tuple2<String, Iterable<Tuple2<String, AvgCount>>> call(Tuple2<String, Iterable<Tuple2<String, AvgCount>>> a) throws Exception {

                    String genre = a._1();


                    //Sort and take top
                    Iterable<Tuple2<String, AvgCount>> it = (Iterable<Tuple2<String, AvgCount>>) a._2();

                    List<Tuple2<String, AvgCount>> lst = new ArrayList<Tuple2<String, AvgCount>>();
                    Iterator<Tuple2<String, AvgCount>> iter = it.iterator();

                    while (iter.hasNext()) {
                        lst.add(iter.next());
                    }

                    //Sort by average
                    Collections.sort(lst, new Comparator<Tuple2<String, AvgCount>>() {
                        public int compare(Tuple2<String, AvgCount> o1, Tuple2<String, AvgCount> o2) {
                            if (o2._2().avg() < o1._2().avg()) return -1;
                            if (o2._2().avg() > o1._2().avg()) return 1;
                            return 0;
                        }
                    });

                    //Check
                    if (lst.size() >= TOP_COUNT)
                        lst= lst.subList(0,TOP_COUNT);

                    Iterable<Tuple2<String, AvgCount>> l = lst;

                    return new Tuple2<String, Iterable<Tuple2<String, AvgCount>>>(genre, l);
                }


            };





}
