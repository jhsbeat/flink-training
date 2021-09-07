package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

public class KeyedStreamExercise {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        rides
          .flatMap(new NYCEnrichment())
          .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
              @Override
              public void flatMap(EnrichedRide ride, Collector<Tuple2<Integer, Minutes>> out) throws Exception {
                  if (!ride.isStart) {
                      Interval rideInterval = new Interval(ride.startTime.toEpochMilli(), ride.endTime.toEpochMilli());
                      Minutes duration = rideInterval.toDuration().toStandardMinutes();
                      out.collect(new Tuple2<>(ride.startCell, duration));
                  }
              }
          })
        .keyBy(value -> value.f0)
        .maxBy(1)
        .print();

        env.execute("Enriched NYC Ride Count");
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
