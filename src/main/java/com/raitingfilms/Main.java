package com.raitingfilms;

import java.text.ParseException;

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

        conroller.calcTopGenresByDiscussedByGender();

        conroller.calcTopGenresByDiscussedByOccupation();
    }


}
