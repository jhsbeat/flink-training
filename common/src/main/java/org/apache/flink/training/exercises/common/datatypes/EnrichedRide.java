package org.apache.flink.training.exercises.common.datatypes;

import java.time.Instant;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

public class EnrichedRide extends TaxiRide {

    public int startCell;
    public int endCell;

    public EnrichedRide() {}

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.eventTime = ride.eventTime;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    @Override
    public String toString() {
        return super.toString() + "," +
          this.startCell + "," +
          this.endCell;
    }
}
