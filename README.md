# Data Dagster

## Goal

The goal is to create a specification and a sample implementation for piping and transforming data.

## Why

None of the current open source and commercial products provides the full set of features of Data Dagster. 

## Features

- Language, OS and cloud agnostic
- Pluggable: a data pipeline consists of steps where each step implements a single interface. 
  This allows anyone to implement data sources, outputs, transformations, logic etc. in various languages, mix them in a single pipeline, deploy each step independently.
  All this together allows greater freedom of development, deployment and scalability.
- A pipeline is a DAG
- Each step of a pipeline runs independently. This allows continuous flow of data.
- API. Can be used to create dependencies among pipelines.
- Scheduler.
- Persistence: data coming into a pipeline step is persisted until successfully output into a next step.
- Centralized logging. This allows centralized monitoring.

## Name

This project is not based on Dagster. It is named Data Dagster because it is allows composition of data DAGs. 

## Design

![design](./images/design.png)

## Roadmap

## Status


