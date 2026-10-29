# Weather Data Pipeline

This project is a weather data pipeline for analysing electricity demand.

Selected states: Karnataka, Maharashtra, and Tripura.

This data pipeline covers the end-to-end design for collecting weather data from trusted and licensed real-world sources. The goal is to produce useful insights that can support electricity demand and supply planning.

The system is designed to gather weather data for any coordinates or region and use it for analysis.

## Expected outcomes

- An analyst at Bhadla Solar Park, Rajasthan, can estimate the solar energy that can be generated tomorrow with a realistic confidence score.
- BESCOM can estimate electricity shortages in the coming month due to reduced supply from Shivanasamudra hydroelectric plant caused by low rainfall.
- Adani Green Energy can estimate output for a new windmill project in Gujarat using wind speed data.

> The examples above show the kind of questions this data warehouse should be able to answer.

## Pipeline architecture

- [Pipeline architecture](docs/design.svg)

> Note: this diagram was generated using excalidraw. (manual design of thoughts, no AI used here)

## Design thinking

### Assumptions

- All four data sources (Open-Meteo, OpenWeatherMap, NASA POWER, and ERA-5) provide data for the planned grid granularity, which is defined as regions.
- There are enough resources and computing capacity to scale the pipeline and warehouse.

### Tradeoffs

- API cost and accuracy of data - data like nasa power can cost us more but with more accuracy, similarly more data = more confidence with more cost (this can be applicable to any of the data providers or third party data sourcing)
- 

### Unfinished parts

- reporting (analysis part) directly from the pipeline results due to memory limitations in the system
- implementation of third party data ingestion in the pipeline


### Addressing critical problems from the problem statement

- get the weather at a place that is not a district, such as a solar park or a city;
  - data is collected from satellite weather monitoring providers, so data is available for all coordinates
  - a remote solar park can be tracked with above data (using coordinates to resolve region or vice versa)

- know where each value came from and how far to trust it;
  - each point of data is tagged with its sources and a realistic confidence reference table to tag a confidence score to ensure how far to trust it

- compare what was forecast with what actually happened;
  - providers has both forecasting data as well as realtime live data (some minimal delay can be expected)
  - which can be used to plot graphs and analyse how far forecast is able to meet realistic numbers
