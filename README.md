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

> Note: this section was generated using a tool.

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