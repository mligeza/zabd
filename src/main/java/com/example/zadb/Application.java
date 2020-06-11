package com.example.zadb;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Indexes.*;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;
import java.util.Arrays;


public class Application {

    public static void main(String[] args) {

        String client = "mongodb://zabd2020:aggregations@db2020-aggregations-shard-00-00-2sawx.mongodb.net:27017/admin?authSource=admin&ssl=true";
        MongoClientURI uri = new MongoClientURI(client);
        MongoClient mongoClient = new MongoClient(uri);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("db-aggregations");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("movies");
        FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> cursor = findIterable.iterator();

        /**
         * ZAD 1
         * DONE
         */
        /*
        Bson project1 = and(
                eq("imdb.rating", 1),
                eq("title", 1),
                eq("languages", 1));

        Bson filter1 = and(
                eq("languages", "English"),
                eq("languages", "German"));

        AggregateIterable<Document> zad1 = mongoCollection.aggregate(Arrays.asList(
                project(project1),
                match(filter1)));

        //showResults(zad1);
*/

        /**
         * ZAD 2
         * ERROR
         */


        //BRAK WARUNKU -  reżyser (pole director) był również aktorem (pole cast)
        // ALE WIEM JAK PRZEROBIC - MOŻNA SKLADAC WARUNKI: https://stackoverflow.com/questions/40714393/aggregation-using-mongodb-java-driver
        Bson project2 = fields(include("directors", "cast","year","imdb.rating","$cast","$directors"), excludeId());

        //Filters.in("directorAsActor","$cast","$directors"


        Bson filter2 = and(
                gt("imdb.rating", 5),
                gt("year", 1960),
                gt("countFilm", 0)
                );

        String count2 = "countFilm";

        BsonArray cond = new BsonArray();

        BsonArray if2 = new BsonArray();
        if2.add(new BsonString("$isArray"));
        if2.add(new BsonString("$directorAsActor"));

        BsonArray then2 = new BsonArray();
        then2.add(new BsonString("$size"));
        then2.add(new BsonString("$directorAsActor"));

        cond.add(new BsonDocument("then", if2));
        cond.add(new BsonDocument("then", then2));
        cond.add(new BsonDocument("else", 0));
/*
        AggregateIterable<Document> zad2 = mongoCollection.aggregate(Arrays.asList(
                project(project2),
                addFields("countFilm",{"$cond":}),
                match(filter2),
                count(count2)));

        showResults(zad2);
*/


        /**
         * ZAD 3
         * DONE
         */




/*
        Bson sort3  = orderBy(descending("filmNum"));

        AggregateIterable<Document> zad3 = mongoCollection.aggregate(Arrays.asList(
                unwind("$cast"),
                group("$cast",sum("filmNum",1),avg("average", "$imdb.rating" )),
                sort(sort3),
                limit(10)

        ));

        showResults(zad3);
*/




    }

    public static void showResults(AggregateIterable<Document> result){
        int i=0;
        for(Object object: result){
            i++;
            if(i>3)break;

            Document document= (Document) object;
            JSONObject jsonObj = new JSONObject(document.toJson());
            System.out.println(jsonObj.toString(4));
        }
    }

}