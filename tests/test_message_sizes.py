import unittest
import spsc_ring_threadsafe as srt



class TestMessageSizes(unittest.TestCase):

	def test_empty_message(self):
		buf = bytearray(1024)
		srt.init(buf)
		srt.put(buf, b"")
		self.assertEqual(srt.get(buf), b"")

	def test_single_byte_messages(self):
		buf = bytearray(1024)
		srt.init(buf)
		for i in range(50):
			srt.put(buf, bytes([i % 256]))
		for i in range(50):
			self.assertEqual(srt.get(buf), bytes([i % 256]))

	def test_mixed_sizes(self):
		buf = bytearray(1024)
		srt.init(buf)
		sizes = [1, 10, 50, 100, 1, 200, 5]
		items = [b"x" * s for s in sizes]
		for item in items:
			srt.put(buf, item)
		for item in items:
			self.assertEqual(srt.get(buf), item)
