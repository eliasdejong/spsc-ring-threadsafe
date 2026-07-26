import unittest
import multiprocessing
import multiprocessing.shared_memory as sm
import spsc_ring_threadsafe as srt



def _producer(shm_name, num_messages, msg_size, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		srt.init(buf)
		ready.set()

		for i in range(num_messages):
			msg = bytes([i % 256]) * msg_size
			while True:
				try:
					srt.put(buf, msg)
					break
				except srt.QueueFullError:
					continue
	finally:
		shm.close()


def _consumer(shm_name, num_messages, msg_size, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		ready.wait()

		for i in range(num_messages):
			expected = bytes([i % 256]) * msg_size
			while True:
				try:
					msg = srt.get(buf)
					if msg != expected:
						raise AssertionError(
							f"Message {i}: expected {expected!r}, got {msg!r}"
						)
					break
				except srt.QueueEmptyError:
					continue
	finally:
		shm.close()


class TestIndexOverflow(unittest.TestCase):
	def test_spsc_32bit_index_overflow(self):
		# 1MB buffer
		buf_size = 1024 * 1024
		# write 50k * 100kB so 5GB total
		num_messages = 50_000
		msg_size = 1024 * 100

		shm = sm.SharedMemory(create=True, size=buf_size)
		try:
			ready = multiprocessing.Event()

			p = multiprocessing.Process(
				target=_producer,
				args=(shm.name, num_messages, msg_size, ready),
			)
			c = multiprocessing.Process(
				target=_consumer,
				args=(shm.name, num_messages, msg_size, ready),
			)

			c.start()
			p.start()
			p.join(timeout=30)
			c.join(timeout=30)

			self.assertFalse(p.is_alive(), "Producer timed out")
			self.assertFalse(c.is_alive(), "Consumer timed out")
			self.assertEqual(p.exitcode, 0, f"Producer crashed: {p.exitcode}")
			self.assertEqual(c.exitcode, 0, f"Consumer failed: {c.exitcode}")
		finally:
			shm.close()
			shm.unlink()
