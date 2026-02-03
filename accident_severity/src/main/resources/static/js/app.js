document.getElementById("predictForm").addEventListener("submit", async (e) => {
    e.preventDefault();

    const body = {
        "DAY_OF_WEEK_CODE": parseInt(document.getElementById("day_code").value),
        "LATITUDE": parseFloat(document.getElementById("lat").value),
        "LONGITUDE": parseFloat(document.getElementById("lon").value),
        "CRASH_HOUR": parseInt(document.getElementById("crash_hour").value),
        "IS_WEEKEND": parseInt(document.getElementById("is_weekend").value)
    };

    const response = await fetch("/predict", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(body)
    });

    const result = await response.json();
    document.getElementById("result").innerText =
        "Predicted Severity: " + result.prediction;
});
