# spsc-ring-threadsafe

**Lockless, thread-safe, single-producer, single-consumer, FIFO queue for Python — implemented on a ring buffer in C.**

![PyPI Downloads](https://img.shields.io/pypi/dm/spsc-ring-threadsafe)
![PyPI Status](https://img.shields.io/pypi/status/spsc-ring-threadsafe)
![PyPI License](https://img.shields.io/pypi/l/spsc-ring-threadsafe)


## Design
Built on a Lamport ring buffer with C11 `_Atomic` read/write indexes (acquire/release ordering). Producer and consumer never contend with eachother.

Features:
- Single producer, single consumer, FIFO semantics
- Lockless (no mutexes or spinlocks)
- Non-blocking / async-friendly
- Suitable for shared memory and IPC
- Compatible with no-GIL & subinterpreters
- Low overhead, especially for small messages
- Up to 100x faster than `multiprocessing.Queue` in the standard library


## Benchmarks
![spsc-ring-threadsafe_benchmarks](https://raw.githubusercontent.com/eliasdejong/spsc-ring-threadsafe/refs/heads/master/spsc-ring-threadsafe_benchmark.png)

---

## Quick Start

```python
import spsc_ring_threadsafe as srt

buf = bytearray(4096)
srt.init(buf)  # initialize as ring buffer

item = b"this is a bytestring message"
srt.put(buf, item)

result = srt.get(buf)
print(result.decode())
```

**Output:**
```
this is a bytestring message
```

### Shared Memory
```python
from multiprocessing import shared_memory
import spsc_ring_threadsafe as srt


shm_name = "app_123456"

# Process 1 (producer)
a = shared_memory.SharedMemory(create=True, size=4096, name=shm_name)
srt.init(a.buf) # initialize as ring buffer

item = b"hello from shared memory!"
srt.put(a.buf, item)
a.close()


# Process 2 (consumer)
b = shared_memory.SharedMemory(name=shm_name)
result = srt.get(b.buf)
print(result.decode())

b.close()
b.unlink()
```

**Output:**
```
hello from shared memory!
```

---

## API Reference

### Exceptions

| Exception | Raised when |
|:---|:---|
| `spsc_ring_threadsafe.QueueFullError` | `put()` called on a full buffer |
| `spsc_ring_threadsafe.QueueEmptyError` | `get()` called on an empty buffer |

### Functions

#### `spsc_ring_threadsafe.init(buf)`

Initialize a mutable buffer as a ring buffer (this function sets the read + write indexes to zero).

| Parameter | Description |
|:---|:---|
| `buf` | Mutable buffer-compatible object (`bytearray`, `memoryview`, etc.). **Size must be a power-of-two anywhere from 256 bytes to 2 GiB.** Also accepts shared memory buffers. |

> ⚠️ **Thread safety:** Initialization is **not** thread-safe. Initialize once before any concurrent access, or provide your own synchronization.

---

#### `spsc_ring_threadsafe.put(buf, item)`

Insert an item into the ring buffer. **Non-blocking.**

| Parameter | Description |
|:---|:---|
| `buf` | Mutable buffer-compatible object. **Size must be a power-of-two anywhere from 256 bytes to 2 GiB.** Also accepts shared memory buffers. |
| `item` | Buffer-compatible object to insert |

**Raises:** `QueueFullError` if the buffer has insufficient space.

> ⚠️ Buffer must be zeroed or initialized before use.

> ⚠️ **Thread safety:** Only a single producer is allowed for a given queue! Multiple producers are NOT thread-safe.

---

#### `spsc_ring_threadsafe.get(buf)`

Remove and return an item from the ring buffer. **Non-blocking.**

| Parameter | Description |
|:---|:---|
| `buf` | Mutable buffer-compatible object. **Size must be a power-of-two anywhere from 256 bytes to 2 GiB.** Also accepts shared memory buffers. |

**Returns:** Buffer-compatible object containing the message.

**Raises:** `QueueEmptyError` if no message is available.

> ⚠️ Buffer must be zeroed or initialized before use.

> ⚠️ **Thread safety:** Only a single consumer is allowed for a given queue! Multiple consumers are NOT thread-safe.

---

## Installation

```bash
uv add spsc-ring-threadsafe
```

or

```bash
pip install spsc-ring-threadsafe
```

---

## Development

### Run tests

```bash
uv run python -m unittest discover -s tests -v
```

### Run benchmarks

```bash
uv pip install -e . --force-reinstall --no-deps
uv run python tests/benchmark.py
```

### Run dev script

```bash
uv pip install -e . --force-reinstall --no-deps
uv run python tests/dev.py
```

---

## LLM Usage Disclosure

Tests and benchmarks were written with assistance from AI models. All C code is written by hand.