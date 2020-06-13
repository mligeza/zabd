package com.example.zadb;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.*;

import static com.mongodb.client.model.Indexes.*;

import org.bson.*;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.json.JSONObject;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;


public class Application {

    public static void main(String[] args) {

        String client = "mongodb://zabd2020:aggregations@db2020-aggregations-shard-00-00-2sawx.mongodb.net:27017/admin?authSource=admin&ssl=true";
        MongoClientURI uri = new MongoClientURI(client);
        MongoClient mongoClient = new MongoClient(uri);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("db-aggregations");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("movies");
        /*FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> cursor = findIterable.iterator();*/

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
         * DONE
         */
/*
        //$count
        String count2 = "countFilm";

        //$addFields
        List<BsonElement> cond= new ArrayList<>();
        BsonDocument e_if= new BsonDocument("$isArray",new BsonString("$directorAsActor"));
        cond.add(new BsonElement("if",e_if));
        BsonDocument e_then= new BsonDocument("$size",new BsonString("$directorAsActor"));
        cond.add(new BsonElement("then",e_then));
        cond.add(new BsonElement("else",new BsonString("0")));
        Field countFilm = new Field("countFilm",new BsonDocument("$cond", new BsonDocument(cond)));




        // $match
        BsonArray and=new BsonArray();
        and.add(new BsonDocument("imdb.rating",new BsonDocument("$gt",new BsonInt32(5))));
        and.add(new BsonDocument("year",new BsonDocument("$gt",new BsonInt32(1960))));
        and.add(new BsonDocument("countFilm",new BsonDocument("$gt",new BsonInt32(0))));
        Bson filter2=new BsonDocument("$and",and);


        //$project
        List<BsonElement> pr2= new ArrayList<>();
        pr2.add(new BsonElement("directors",new BsonInt32(1)));
        pr2.add(new BsonElement("cast",new BsonInt32(1)));
        pr2.add(new BsonElement("year",new BsonInt32(1)));
        pr2.add(new BsonElement("imdb.rating",new BsonInt32(1)));
        BsonArray temp2array=new BsonArray();
        temp2array.add(new BsonString("$cast"));
        temp2array.add(new BsonString("$directors"));
        BsonElement directorAsActor = new BsonElement("directorAsActor",new BsonDocument("$setIntersection",temp2array));
        pr2.add(directorAsActor);
        Bson projection = new BsonDocument(pr2);

        //aggregate
        AggregateIterable<Document> zad2 = mongoCollection.aggregate(Arrays.asList(

                project(projection), //OK
                addFields(countFilm), //OK
                match(filter2),//OK
                count(count2)//OK

        ));


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

        /**
         * ZAD 4
         * DONE
         */
/*
        MongoCollection<Document> mongoCollectionAirlines = mongoDatabase.getCollection("air_alliances");



        //$match
        List<BsonValue> f4= new ArrayList<>();
        f4.add(new BsonString("BCN"));
        Bson filter4 = new BsonDocument("routes.dst_airport", new BsonDocument("$in",new BsonArray(f4)));

        //$group
        BsonField group4=new BsonField("routes_count",new BsonDocument("$sum",new BsonInt32(1)));

        //$sort
        Bson sort4= new BsonDocument("routes_count",new BsonInt32(-1));


        AggregateIterable<Document> zad4 = mongoCollectionAirlines.aggregate(Arrays.asList(
                unwind("$airlines"),
                lookup("air_routes","airlines","airline.name","routes"),
                unwind("$routes"),
                match(filter4),
                group("$name",group4),
                sort(sort4),
                limit(1)


        ));

        showResults(zad4);
*/

/**
 * ZAD 5
 * DONE
 */

/*
        MongoCollection<Document> mongoCollectionAirRoutes = mongoDatabase.getCollection("air_routes");



        //$match
        Bson filter5= and(
                eq("airline.name","Lufthansa"),
                eq("src_airport", "BCN")
        );


        //graphlookupoptions
        GraphLookupOptions glo5 =new GraphLookupOptions();
        glo5.depthField("hops");
        glo5.restrictSearchWithMatch(new BsonDocument("airline.name",new BsonString("Lufthansa")));
        glo5.maxDepth(2);

        //$add
        List<BsonElement> f5= new ArrayList<>();
        f5.add(new BsonElement("input",new BsonString("$przesiadka")));
        BsonArray eq5=new BsonArray();
        eq5.add(new BsonString("$$this.hops"));
        eq5.add(new BsonInt32(2));
        f5.add(new BsonElement("cond",new BsonDocument("$eq",eq5)));
        Field przesiadka = new Field("przesiadka",new BsonDocument("$filter",new BsonDocument(f5)));


        AggregateIterable<Document> zad5 = mongoCollectionAirRoutes.aggregate(Arrays.asList(
                match(filter5),
                graphLookup("air_routes","$src_airport","dst_airport","src_airport","przesiadka",glo5),
                addFields(przesiadka)
        ));

        showResults(zad5);
*/

        /**
         * ZAD 9
         *
         */
        /*
        MongoCollection<Document> mongoCollectionAirRoutes = mongoDatabase.getCollection("air_routes");


        Bson filter9 = new BsonDocument("src_airport",new BsonString("DUS"));

        List<Facet> facets = new ArrayList<>();
        List<Bson> f_DUStoWAW_pipeline = new ArrayList<>();
        f_DUStoWAW_pipeline.add(new BsonDocument("$match", new BsonDocument("dst_airport",new BsonString("WAW"))));

        List<Bson> f_DUStoPOZ_pipeline = new ArrayList<>();
        f_DUStoPOZ_pipeline.add(new BsonDocument("$match", new BsonDocument("dst_airport",new BsonString("POZ"))));

        Facet f_DUStoWAW = new Facet("DUStoWAW",f_DUStoWAW_pipeline);
        Facet f_DUStoPOZ = new Facet("DUStoWAW",f_DUStoPOZ_pipeline);
        facets.add(f_DUStoWAW);
        facets.add(f_DUStoPOZ);

        AggregateIterable<Document> zad9 = mongoCollectionAirRoutes.aggregate(Arrays.asList(
                match(filter9),
                facet(facets)
        ));
*/

    }

    public static void showResults(AggregateIterable<Document> result){
        int i=0;
        for(Object object: result){
            i++;
            if(i>3)break;

            Document document= (Document) object;
            JSONObject jsonObj = new JSONObject(document.toJson());
            System.out.println(jsonObj.toString(2));
        }
    }



}