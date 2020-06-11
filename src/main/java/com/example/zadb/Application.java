package com.example.zadb;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import static com.mongodb.client.model.Aggregates.*;


public class Application {

    public static void main(String[] args) {

        String client = "mongodb://zabd2020:aggregations@db2020-aggregations-shard-00-00-2sawx.mongodb.net:27017/admin?authSource=admin&ssl=true";
        MongoClientURI uri = new MongoClientURI(client);
        MongoClient mongoClient = new MongoClient(uri);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("db-aggregations");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("movies");
        FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> cursor = findIterable.iterator();

        Bson project = and(
                eq("imdb.rating", 1),
                eq("title", 1),
                eq("languages", 1));

        Bson filter = and(
                eq("languages", "English"),
                eq("languages", "German"));

        AggregateIterable<Document> zad1 = mongoCollection.aggregate(Arrays.asList(
                project(project),
                match(filter)));

        showResults(zad1);




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