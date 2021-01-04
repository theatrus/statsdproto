# statsdproto
Utilities to deal with parsing and canonicalizing statsd protocol messages

This library is intended to be used in high volume statsd applications. As such,
it has several important properties:

- A Protocol Data Unit (PDU) low level construct which has parsed and split
  statsd line messages, but performs no further parsing or interpretation of the
  message, simply exposing the fields as a series of slices.
- Usage of accelerated scanning by way of the `memchr` crate to find fields.
- Support and extraction of all known statsd variants, including:
  - DogStatsD format with optional tags
  - Inline tags ("Lyft style").
  - Sampling ratios
  - All possible data types (data type agnostic)
  - Tolerant of nearly all character values in the statsd name, including `:`
    (for those Prometheus cases) - implements a reverse scanner.
- Higher level operations to parse statsd frames into proper Rust objects
- Support for "canonicalization", that is ordering of tags and fields in a well
  defined order.
- Benchmark support with Criterion

This library does not implement any socket code and is purely a parsing library.
It does not implement new-line splitting of datagram splitting.

