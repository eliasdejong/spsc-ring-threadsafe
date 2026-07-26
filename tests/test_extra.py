import unittest
import spsc_ring_threadsafe as srt



class TestExtra(unittest.TestCase):
	
	def test_fill_drain_refill(self):
		buf = bytearray(256)
		srt.init(buf)
		items = []
		i = 0
		while True:
			item = f"item{i}".encode()
			try:
				srt.put(buf, item)
				items.append(item)
			except srt.QueueFullError:
				break
			i += 1
			if i > 1000:
				self.fail("Buffer should have filled")
		
		for item in items:
			self.assertEqual(srt.get(buf), item)
		
		for item in items:
			srt.put(buf, item)
		
		for item in items:
			self.assertEqual(srt.get(buf), item)

	def test_interleaved_producer_consumer(self):
		buf = bytearray(256)
		srt.init(buf)
		# Pattern: put 4, get 2, put 3, get 5, put 2, get 2
		batch1 = [f"b1-{i}".encode() for i in range(4)]
		batch2 = [f"b2-{i}".encode() for i in range(3)]
		batch3 = [f"b3-{i}".encode() for i in range(2)]
		
		for item in batch1:
			srt.put(buf, item)
		self.assertEqual(srt.get(buf), batch1[0])
		self.assertEqual(srt.get(buf), batch1[1])
		
		for item in batch2:
			srt.put(buf, item)
		
		# Should have batch1[2], batch1[3], batch2[0], batch2[1], batch2[2]
		expected = batch1[2:] + batch2
		for item in expected:
			self.assertEqual(srt.get(buf), item)
		
		for item in batch3:
			srt.put(buf, item)
		for item in batch3:
			self.assertEqual(srt.get(buf), item)

	def test_many_wraparound_cycles(self):
		buf = bytearray(256)
		srt.init(buf)
		# Run enough cycles to force wraparounds
		for cycle in range(2000):
			# Put a few
			for i in range(3):
				srt.put(buf, f"c{cycle}-{i}".encode())
			# Get a few
			for i in range(3):
				self.assertEqual(srt.get(buf), f"c{cycle}-{i}".encode())
