import unittest
import threading
import spsc_ring_threadsafe as srt



class TestThreading(unittest.TestCase):

	def test_spsc_stress(self):
		buf = bytearray(1024)
		srt.init(buf)
		num_messages = 1000
		produced = []
		consumed = []
		errors = []

		def producer():
			for i in range(num_messages):
				msg = f"msg-{i:05d}".encode()
				while True:
					try:
						srt.put(buf, msg)
						produced.append(msg)
						break
					except srt.QueueFullError:
						continue

		def consumer():
			for _ in range(num_messages):
				while True:
					try:
						msg = srt.get(buf)
						consumed.append(msg)
						break
					except srt.QueueEmptyError:
						continue

		t1 = threading.Thread(target=producer)
		t2 = threading.Thread(target=consumer)
		t1.start()
		t2.start()
		t1.join(timeout=30)
		t2.join(timeout=30)

		self.assertEqual(len(produced), num_messages)
		self.assertEqual(len(consumed), num_messages)
		self.assertEqual(produced, consumed)


	def test_spsc_mixed_sizes(self):
		buf = bytearray(4096)
		srt.init(buf)
		num_messages = 1000
		sizes = [1, 10, 50, 100, 200]

		def producer():
			for i in range(num_messages):
				size = sizes[i % len(sizes)]
				msg = bytes([i % 256] * size)
				while True:
					try:
						srt.put(buf, msg)
						break
					except srt.QueueFullError:
						continue

		def consumer():
			for i in range(num_messages):
				while True:
					try:
						msg = srt.get(buf)
						expected = bytes([i % 256] * sizes[i % len(sizes)])
						self.assertEqual(msg, expected)
						break
					except srt.QueueEmptyError:
						continue

		t1 = threading.Thread(target=producer)
		t2 = threading.Thread(target=consumer)
		t1.start()
		t2.start()
		t1.join(timeout=30)
		t2.join(timeout=30)