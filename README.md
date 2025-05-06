# Crater

## Crater

Crater is a simple, fast, flat namespaced [1] key value store written in python. It allows concurrently locked read/writes, list operations, and collision

##  Versionstamp

Versionstamp is a monotonically increasing, collision resistant, time ordered unique identifier. 

### Specification

```
 0006346f4104bef1    0000        37b0
|----------------|  |----|      |----|
    Timestamp        Counter     Randomness
    64 bits          16 bits     16 bits
```

### Components

#### Timestamp

* 64 bit integer
* UNIX in milliseconds

#### Counter

* Counter incremented if (within the thread Lock) the make function is called within the same millisecond for collision resistance

#### 

* 16 Bits of randomness to ensure uniqueness

### Canonical  String Representation

```
ttttttttttttccccrrrr 

where 
t is timestamp 
c is counter 
r is randomness 
```

