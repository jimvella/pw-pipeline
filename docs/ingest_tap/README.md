# ingest_tap - Frame Stream Monitor

## Overview

`ingest_tap` is a TUI (terminal user interface) for real-time monitoring of frame streams flowing through the pipeline. It provides live visibility into what frames are being processed.

## Purpose

**Real-time debugging**: See frames as they're processed

**Use cases**:
- Verify receiver connectivity
- Monitor frame types
- Debug decode issues
- Watch specific aircraft

## Usage

```bash
# Monitor NATS ingest tap
ingest_tap --nats nats://localhost:4222

# Monitor with filters
ingest_tap --nats nats://localhost:4222 --icao A12345
```

## TUI Interface

**Display**:
- Frame rate (frames/sec)
- Frame types (DF 17, DF 11, etc.)
- Aircraft ICAO addresses
- Decode errors
- Scrolling frame list

**Keyboard controls**:
- `q`: Quit
- Arrow keys: Scroll
- `/`: Search/filter

## How It Works

**IngestTap middleware**: Publishes frames to NATS topic

**ingest_tap subscribes**: Receives real-time feed

**Keeps its tap open**: Each pipeline instance drops a tap condition 5 minutes after its last `add`, so `ingest_tap` repeats its `add` every minute. On exit it stops refreshing, waits for any refresh in flight, then sends `remove`. If it is killed, the tap expires on its own within 5 minutes.

**No impact**: Read-only monitoring (doesn't affect pipeline)

## See Also

- [middleware](../middleware/README.md) - IngestTap implementation
- [tracker](../tracker/README.md) - Frame processing
