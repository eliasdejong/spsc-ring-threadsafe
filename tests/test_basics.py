import unittest
import spsc_ring_threadsafe as srt



class TestBasics(unittest.TestCase):

	def test_single(self):
		buf = bytearray(1024)
		srt.init(buf)
		item = b"test item"
		srt.put(buf, item)
		result = srt.get(buf)
		self.assertEqual(item, result)

	def test_batch_3(self):
		buf = bytearray(1024)
		srt.init(buf)
		srt.put(buf, b"first")
		srt.put(buf, b"second")
		srt.put(buf, b"third")
		self.assertEqual(srt.get(buf), b"first")
		self.assertEqual(srt.get(buf), b"second")
		self.assertEqual(srt.get(buf), b"third")

	def test_batch_10(self):
		buf = bytearray(1024)
		srt.init(buf)
		messages = [f"this is message number {i}".encode() for i in range(10)]
		for m in messages:
			srt.put(buf, m)
		for m in messages:
			self.assertEqual(srt.get(buf), m)

	def test_batch_100(self):
		buf = bytearray(4096)
		srt.init(buf)
		messages = [f"this is message number {i}".encode() for i in range(100)]
		for m in messages:
			srt.put(buf, m)
		for m in messages:
			self.assertEqual(srt.get(buf), m)

	def test_stream_3(self):
		buf = bytearray(1024)
		srt.init(buf)
		for i in range(3):
			message = f"this is message number {i}".encode()
			srt.put(buf, message)
			self.assertEqual(srt.get(buf), message)

	def test_stream_10(self):
		buf = bytearray(1024)
		srt.init(buf)
		for i in range(10):
			message = f"this is message number {i}".encode()
			srt.put(buf, message)
			self.assertEqual(srt.get(buf), message)

	def test_stream_100(self):
		buf = bytearray(1024)
		srt.init(buf)
		for i in range(100):
			message = f"this is message number {i}".encode()
			srt.put(buf, message)
			self.assertEqual(srt.get(buf), message)
	
	def test_stream_10000(self):
		buf = bytearray(1024)
		srt.init(buf)
		for i in range(10000):
			message = f"this is message number {i}".encode()
			srt.put(buf, message)
			self.assertEqual(srt.get(buf), message)

	def test_raise_empty(self):
		buf = bytearray(1024)
		srt.init(buf)
		with self.assertRaises(srt.QueueEmptyError):
			result = srt.get(buf)

	def test_raise_small_buffer(self):
		buf = bytearray(255)
		with self.assertRaises(ValueError):
			srt.init(buf)

	def test_raise_non_pow2(self):
		buf = bytearray(1011)
		with self.assertRaises(ValueError):
			srt.init(buf)
