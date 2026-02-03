package com.company.accident;

public class TestPrediction {
    public static void main(String[] args) {

        PredictionService service = new PredictionService();

        // SAMPLE INPUT
        int DAY_OF_WEEK_CODE = 3;    // Wednesday
        double LATITUDE = 40.7128;
        double LONGITUDE = -74.0060;
        int CRASH_HOUR = 14;
        int IS_WEEKEND = 0;          // weekday

        double prediction = service.predict(
                DAY_OF_WEEK_CODE,
                LATITUDE,
                LONGITUDE,
                CRASH_HOUR,
                IS_WEEKEND
        );

        System.out.println("Prediction = " + prediction);
    }
}
