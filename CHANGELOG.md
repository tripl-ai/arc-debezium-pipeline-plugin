# Change Log

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
