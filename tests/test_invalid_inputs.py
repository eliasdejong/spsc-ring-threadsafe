import unittest
import spsc_ring_threadsafe as srt



class TestInvalidInputs(unittest.TestCase):

	def test_put_non_bytes(self):
		buf = bytearray(256)
		srt.init(buf)
		with self.assertRaises(TypeError):
			srt.put(buf, "not bytes")
		with self.assertRaises(TypeError):
			srt.put(buf, 123)
		with self.assertRaises(TypeError):
			srt.put(buf, None)

	def test_uninitialized_buffer_get(self):
		buf = bytearray(256)
		with self.assertRaises(srt.QueueEmptyError):
			srt.get(buf)

	def test_init_twice(self):
		buf = bytearray(256)
		srt.init(buf)
		srt.put(buf, b"first")
		srt.init(buf)  # should reset
		with self.assertRaises(srt.QueueEmptyError):
			srt.get(buf)
		srt.put(buf, b"second")
		self.assertEqual(srt.get(buf), b"second")

	def test_immutable_buffer(self):
		with self.assertRaises(BufferError):
			srt.init(bytes(256))