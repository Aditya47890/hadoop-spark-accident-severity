package com.company.accident;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class AccidentController {

    @Autowired
    private PredictionService predictionService;

    @PostMapping("/predict")
    public PredictionResponse predict(@RequestBody PredictionRequest req) {

        double pred = predictionService.predict(
                req.DAY_OF_WEEK_CODE,
                req.LATITUDE,
                req.LONGITUDE,
                req.CRASH_HOUR,
                req.IS_WEEKEND
        );

        return new PredictionResponse(pred);
    }
}

