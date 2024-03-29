# Change Log

## 1.5.0

- bump to Arc 3.13.2.

## 1.4.0

- bump to Arc 3.7.0.

## 1.3.2

- add support for `oracle` which has mostly the same behavior as the `mysql`/`postgres`.

## 1.3.1

- **FIX** add `_topic` and `_offset` back into output.

## 1.3.0

- **FIX** timezone handling for non specific types (i.e. MySQL `DATETIME`).
- add ability to parse `precise` (i.e. `base64` encoded) decimal and bigint types.
- better error messages on unexpected values.

## 1.2.0

- add `_topic` and `_offset` values to output schema.
- **FIX** allow `boolean` stored as integer to be parsed.

## 1.1.3

- **FIX** allow `timestamp` for MySQL to support millisecond timestamps.

## 1.1.2

- **FIX** ensure schema metadata is set to be consistent with other Arc `*Extract` stages.

## 1.1.1

- **FIX** correct the code snippet for use with Arc Jupyter.

## 1.1.0

- remove constraint to only support `streaming` mode.
- add support for injecting a previous state by providing `initialStateView` and `initialStateKey`. currently only supports single value keys.
- add support for `persist`, `numPartitions` and `partitionBy` to align with standard Arc extract stages.

## 1.0.0

- initial release.
