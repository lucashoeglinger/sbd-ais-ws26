## 1. Decision paragraph

For this scenario, log-based CDC is the optimal choice because it safely guarantees complete, strictly-ordered event delivery without requiring any code changes to the certified payment path. By reading directly from PostgreSQL's write-ahead log, it completely avoids the out-of-order omissions of polling and the lost or phantom records inherent to dual writes. The primary architectural cost of this choice is the reliance on a replication slot, which forces PostgreSQL to retain all unread WAL files on disk. To prevent a stalled connector from filling the disk and halting the entire payment database, you would actively monitor the replication slot's lag and the database's available disk space, configuring alerts to fire long before capacity is exhausted. However, if my team owned the payment path and could safely modify the application code, I would implement the Outbox pattern instead. Writing both the payment and an event record within the same database transaction ensures atomic consistency at the minimal cost of one extra row per transaction, bypassing the operational risks of managing database replication slots altogether.

## 2. The 3 runs

### 2.1 Run 1

> Predictions: We did not set a partition count, so Debezium decides. If the topic gets a single partition, only one of the three velocity instances can read it, because within a consumer group each partition is read by exactly one instance. The other two would sit idle. One instance handles at most ~500 payments/s of the 1,000, so its lag would grow and R4 should fail. The anomaly model needs only one instance, so it should pass.

> Results: The velocity model only catches 2/6 fraudulent caught. It never gave false alert and the R4 fails, p95 68.7 s. This is due to the topic having 1 partition (partitions 1, members 3), so only vel-1 reads and its lag grows to about 70,000 events, while vel-2 and vel-3 never get a partition. the others just sit there. In the kafka UI it became apparent that the key column is the `id` of an event. (found out by comparing the payload with the key column (which was an Integer))


``` bash
Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   1        1     1,601
  velocity-agent           fraud.public.transactions                   1        3    69,817

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1        2863645 anomaly         130,482       72,886             0      0.6       3s ago
  vel-1        2863646 velocity         55,860       42,774             0     68.6       2s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          2/6  (33%)   FAIL  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  57.4 s / 68.7 s   FAIL  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          6/6  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.5 s / 0.6 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)
```

### 2.2 Run 2
> Predictions: Debezium's default key is the table's primary key `id`. Since `id` is unique, the key hash spreads payments evenly over the six partitions, like having no key. All three instances will get two partitions each and keep up, so R4 should pass. But the 12 test payments of one stolen card land on different partitions, so on different instances. Each instance sees only about 4 of them, below the threshold of 8, so R3 for the velocity model should fail. The key would have to be the cardholder (`user_id`), the entity whose state the model keeps.

> Results: As expected in predictions the velocity (card testing fraud) was not caught with great success. Since the parition key is `id`, each instance gets only `12/3 = 4` out of the cards 12 transactions, which is way below the models threshold of `8` (This is the average because the hash is random as this point, there is still a chance of putting more than `8` on a partition), The R3 is higher than expected, this could be due to chance but it seems unlikely. We do now have 6 partitions instead of 1. Now the parittions get distributed accross all 3 velocity instances. Leading to the p95 latency checks to pass.

```bash

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1       427
  velocity-agent           fraud.public.transactions                   6        3       424

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1        2920226 anomaly         168,112       81,402             0      0.6       0s ago
  vel-1        2920227 velocity         49,795       39,185             0      0.6       5s ago
  vel-2        2920228 velocity         49,642       39,084             1      0.6       5s ago
  vel-3        2920229 velocity         50,938       39,897             1      0.6       5s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          3/8  (38%)   FAIL  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.6 s / 1.0 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          9/9  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.6 s / 0.9 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

```


### 2.3 Run 3

> Prediction: With `message.key.columns` set to `user_id`, the key of an event becomes `{"user_id": ...}` instead of `{"id": ...}`. All payments of one cardholder hash to the same partition, so one instance sees all 12 test payments and alerts at the 8th. R3 for the velocity model should pass, and R4 should stay as in Run 2.

> Results: As expected the Key column worked to catch card fraud. This was due to us previously storing with the `id` primary key so the users got stored in some "random" partition and the detector not finding the abnormality. This is due to our card detector now seing all 12 events in the same partition instead of them being scattered accross all 3 detectors.

```bash

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1     1,566
  velocity-agent           fraud.public.transactions                   6        3     1,585

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1        2938094 anomaly         127,379       72,025             0      1.1       2s ago
  vel-1        2938095 velocity         39,884       23,338             0      0.9       0s ago
  vel-2        2938096 velocity         39,840       23,230             0      1.0       1s ago
  vel-3        2938097 velocity         40,746       24,150             0      0.9       0s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          6/6  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  1.0 s / 1.1 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          6/6  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.9 s / 1.0 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

```



## 3. Closing answer

Debezium left two defaults that I had to change: the topic got 1 partition
(nobody set a count), and the key of an event was the table's primary key `id`. Neither raised an error.

With 1 partition, only `vel-1` reads the topic and handles about 500 of the 1,000
payments/s, while `vel-2` and `vel-3` sit idle. In production the lag would grow
without bound (about 70,000 events and a p95 of 68.7 s in my Run 1), so alerts
would come too late to block the thief's next payment (R4), and frauds of the
last minutes would not be caught yet. With the key `id` and 6 partitions, the 12
test payments of one stolen card spread over all three instances, each sees about
4, below the threshold of 8, and R3 fails (38 % in Run 2). The fraud model would
just catch fewer frauds, with no crash and healthy-looking lag.

The partition problem is easy to notice: growing consumer lag, 3 group members
but 1 partition, and agents that print `assigned partitions []`. The key problem
is silent. A team would only find it by measuring the catch rate, for example by
injecting a synthetic card-testing attack and checking that it alerts, or by
comparing alerts with confirmed chargebacks. A check before go-live helps too:
the key must be the entity whose state the model keeps (`user_id`), which one
look at the key in the Kafka UI shows.

