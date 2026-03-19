# Time to Purchase Funnel Analysis

## Project Overview

This project analyzes user conversion behavior using event-level web analytics data.

The focus is on understanding how long it takes users to make a purchase after arriving on the website, as well as identifying key factors that influence conversion speed and user progression through the purchase journey.

The analysis combines SQL-based data transformation with Power BI visualization to explore patterns in user behavior and support data-driven decision making.

## Business Questions

The analysis is designed to answer the following questions:

- How long does it take users to convert after their first interaction?
- What proportion of users convert quickly versus over longer time periods?
- How does time-to-purchase differ between first-time and returning customers?
- Do marketing campaigns influence conversion speed?
- How do user and session characteristics (device, browser, etc.) affect purchase behavior?
- Where do users drop off in the conversion funnel?

## Dataset

The dataset consists of event-level web analytics data structured similarly to a Google Analytics 4 export.

Each row represents a user interaction event and includes information such as:

- event type and timestamp
- anonymous user identifier
- marketing attribution fields (source, medium, campaign)
- device and browser metadata
- ecommerce purchase data

Because some purchase events contain `(data deleted)` in the campaign field, campaign attribution was derived at the **session level** using the first valid campaign observed within each session.

## Tools & Technologies

SQL (BigQuery) — data extraction, transformation, and feature engineering

Power BI — data visualization and dashboard creation

## Methodology

The project follows these main steps:

1. Data preparation and cleaning of raw event-level data
2. Construction of purchase-level metrics, including time-to-purchase
3. Aggregation of user and session attributes
4. Analysis of conversion behavior across different segments
5. Visualization of results in an interactive dashboard