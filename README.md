UBC CPSC416 / MIT s6.5840 Project

## Raft Consensus Algorithm (L2)

- ☑️Leader Election (L2A)
- ☑️Commit (L2B)
- ☑️Persist (L2C)
- ☑️Snapshot (L2D)

<details>
  <summary>Test results (one of 100 passes) </summary>
  
```
Test (2A): initial election ...
  ... Passed --   3.0  3   60   16440    0
Test (2A): election after network failure ...
  ... Passed --   4.4  3  131   24748    0
Test (2A): multiple elections ...
  ... Passed --   5.6  7  652  124444    0
Test (2B): basic agreement ...
  ... Passed --   0.3  3   14    3895    3
Test (2B): RPC byte count ...
  ... Passed --   1.1  3   46  163597   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.2  3  131   25730    3
Test (2B): test failure of leaders ...
  ... Passed --   4.6  3  184   39779    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   3.4  3   95   23640    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  237   45776    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.5  3   16    4247    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   5.5  3  180   43802    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  18.2  5 1873 1371812  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   44   12853   12
Test (2C): basic persistence ...
  ... Passed --   4.5  3  114   29608    7
Test (2C): more persistence ...
  ... Passed --  20.9  5 1403  274158   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   0.8  3   31    7921    4
Test (2C): Figure 8 ...
  ... Passed --  36.2  5 1233  235905   23
Test (2C): unreliable agreement ...
  ... Passed --   6.5  5  521  168080  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  38.6  5 3660 14813397  377
Test (2C): churn ...
  ... Passed --  16.2  5  817  470770  235
Test (2C): unreliable churn ...
  ... Passed --  16.1  5 1087  756793  266
Test (2D): snapshots basic ...
  ... Passed --   3.7  3  136   55045  228
Test (2D): install snapshots (disconnect) ...
  ... Passed --  65.2  3 1555  561343  348
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  71.5  3 2043  652621  328
Test (2D): install snapshots (crash) ...
  ... Passed --  65.8  3 1424  487356  315
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  69.9  3 1789  619934  362
Test (2D): crash and restart all servers ...
  ... Passed --   7.1  3  280   83229   67
Test (2D): snapshot initialization after crash ...
  ... Passed --   1.5  3   62   17964   14
PASS
ok  	cpsc416/raft	481.302s
```

</details>


## Shared Key-Value Store (No sharding) (L3)
- L3A
- L3B
