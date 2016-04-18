package com.raitingfilms;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
/**
 * Created by kost on 4/18/16.
 */
public class MongoDbSaver {

    public static MongoDatabase db;
    public static MongoClient mongoClient;


    public String save(List<Tuple2<String, Integer>> listFilm) throws ParseException {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");

        List<Document> documents = new ArrayList<Document>();

        for (Tuple2<String, Integer> t :listFilm) {
            String tmp = t._1;
            documents.add(new Document("nameFilm" , tmp));
        }
        //DBCollection u = db.getCollection("totalTop");
        db.getCollection("totalTop").insertMany(documents);

        return "Recording is finish";
    }
}
