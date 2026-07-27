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