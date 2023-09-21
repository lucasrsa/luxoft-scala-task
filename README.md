# Luxoft Scala Task #

## Description ##

Provide example of integration of Spark job with web Service (can be provided as part of exercise or already existing one) for fetching stock prices and calculating hourly statistics of min, max, average of price per stock.

## Issues ##

A combination of the limitations of free accounts on the available web services and the processing limits of my personal computer made it challenging to work with a decently sized dataset. Since the main target of this task was to evaluate my coding style mainly focused on the functional programming side, I believed it would be possible to work with a "fake" dataset created during runtime.

## Solution ##

The "fake" dataset consists of 13824 different stocks with price information for every minute of the first 6 hours of the day. This results in a total of 4976640 different prices created randomly (ranging from 100.00 to 200.00).

## Build & Run ##

```sh
$ cd luxoft-scala-task
$ sbt
> run
```
