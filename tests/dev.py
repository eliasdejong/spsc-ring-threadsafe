import spsc_ring_threadsafe as srt

buf = bytearray(4096)
srt.init(buf) # initialize as ring buffer

item = b"this is a bytestring message"
srt.put(buf, item)


result = srt.get(buf)
print(result.decode())