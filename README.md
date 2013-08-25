almaz
=====

*Carbon*-like metric collection daemon which stores all data in memory and does not use disk.

Install
-------

This repository contains a Go workspace. Clone it...
```
git clone https://github.com/Babazka/almaz.git
```

Compile and run:
```
make
bin/almaz
```

*Almaz* will listen on port 7701 for metrics, and on port 7702 for data queries. Ports can be changed with command-line options.

Submitting metrics to almaz
---------------------------

*Almaz* receives metrics using the same protocol *Carbon* uses. Send metrics over plain TCP, one line per metric:
```
stats_counts.adv.shows.429.2005.4186 16 1377447313
statsd.numStats 5001 1377447313
stats.statsd.graphiteStats.calculationtime 18 1377447313
stats.statsd.processing_time 2 1377447313
stats.statsd.graphiteStats.last_exception 1377435453 1377447313
stats.statsd.graphiteStats.last_flush 1377446503 1377447313
stats.statsd.graphiteStats.flush_time 21 1377447313
stats.statsd.graphiteStats.flush_length 471582 1377447313
```
Lines are separated by `\n`. Each line contains three fields -- metric name (string without spaces), metric value (float), and Unix timestamp (integer), separated by one space. You can submit an arbitrary number of metrics in a single connection.

Almaz backend for statsd
------------------------

 * Put following settings in your `statsd` config file.
```
, almazPort: 7701
, almazHost: "localhost" 
```
 * Copy `statsd_backend/almaz.js` file from this repository to `backends/` directory in your *statsd* installation.
 * Restart your *statsd* daemon.
