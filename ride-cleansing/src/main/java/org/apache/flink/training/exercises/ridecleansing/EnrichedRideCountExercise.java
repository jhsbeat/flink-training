package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.apache.flink.util.Collector;

public class EnrichedRideCountExercise {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        // Case 1 : filter & map
//        DataStream<EnrichedRide> enrichedNYCRides = rides
//          .filter(new RideCleansingSolution.NYCFilter())
//          .map(new Enrichment());

        // Case 2 : flatMap
        DataStream<EnrichedRide> enrichedNYCRides = rides
          .flatMap(new NYCEnrichment());

        enrichedNYCRides.print();

        env.execute("Enriched NYC Ride Count");
    }

    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {

        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }

    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = new RideCleansingSolution.NYCFilter();
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }
}
