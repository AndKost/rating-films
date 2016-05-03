package com.raitingfilms;

import com.raitingfilms.mainjobs.extra.AvgCount;
import scala.Tuple2;

import java.text.ParseException;
import java.util.List;

public class Main {

    public static void main(String[] args) throws ParseException {

        JobConroller conroller = new JobConroller();

        conroller.calcTotalTopFilms();

        conroller.calcTotalTopFilmsByGenre();

        conroller.calcTotalTopGenreByUsers();

        conroller.calcTotalTopFilmsByAge();

        //Option jobs
        conroller.calcTopFilmsByGender();

        conroller.calcMostDiscussedFilmsByGender();

        conroller.calcTopFilmsByOccupation();

        conroller.calcMostDiscussedFilmsByOccupation();

        conroller.calcTopFilmsByYearByGenre();
    }


}
