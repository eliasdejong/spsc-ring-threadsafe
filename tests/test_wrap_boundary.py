import unittest
import spsc_ring_threadsafe as srt


class TestWrapBoundary(unittest.TestCase):
	"""Deterministic pad-boundary test for the watermark wrap protocol.

	Fills the ring to capacity (forcing the producer through the pad path),
	then drains and refills in lockstep so the producer wraps repeatedly
	while the consumer lags at various offsets. Verifies FIFO order across
	the pad and that the ring recovers cleanly afterwards.
	"""

	def test_pad_boundary_order(self):
		for bufsz in (256, 512, 1024, 4096):
			for msg_size in range(0, 60):
				with self.subTest(bufsz=bufsz, msg_size=msg_size):
					buf = bytearray(bufsz)
					srt.init(buf)
					msg = b"a" * msg_size

					# Fill until full.
					n_filled = 0
					while True:
						try:
							srt.put(buf, msg)
							n_filled += 1
						except srt.QueueFullError:
							break
					self.assertGreater(n_filled, 0)

					# Drain half, verifying FIFO.
					for _ in range(n_filled // 2):
						self.assertEqual(srt.get(buf), msg)

					# Lockstep drain/refill: consume the remaining original
					# items one at a time while putting new ones back, so the
					# producer wraps (and pads) while the consumer lags.
					n_remaining = n_filled - n_filled // 2
					new_msg = b"b" * msg_size
					for _ in range(n_remaining):
						# Get first (frees space), then put: the consumer lags
						# behind the producer across the pad boundary.
						self.assertEqual(srt.get(buf), msg)
						srt.put(buf, new_msg)

					# Drain the new items, verifying FIFO.
					for _ in range(n_remaining):
						self.assertEqual(srt.get(buf), new_msg)

					# Ring must be empty and reusable after the wraps.
					with self.assertRaises(srt.QueueEmptyError):
						srt.get(buf)
					for i in range(100):
						item = b"recovery-%d" % i
						srt.put(buf, item)
						self.assertEqual(srt.get(buf), item)
					with self.assertRaises(srt.QueueEmptyError):
						srt.get(buf)


if __name__ == "__main__":
	unittest.main()
