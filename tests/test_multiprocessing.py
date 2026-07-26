import unittest
import multiprocessing
import multiprocessing.shared_memory as sm
import spsc_ring_threadsafe as srt


def _producer(shm_name, num_messages, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		srt.init(buf)
		ready.set()

		for i in range(num_messages):
			msg = f"msg-{i:08d}".encode()
			while True:
				try:
					srt.put(buf, msg)
					break
				except srt.QueueFullError:
					continue
	finally:
		shm.close()


def _consumer(shm_name, num_messages, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		ready.wait()

		for i in range(num_messages):
			expected = f"msg-{i:08d}".encode()
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


def _producer_mixed(shm_name, num_messages, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		srt.init(buf)
		ready.set()
		sizes = [1, 1, 1, 3, 5, 10, 10, 50, 100, 200]
		for i in range(num_messages):
			size = sizes[i % len(sizes)]
			msg = bytes([i % 256] * size)
			while True:
				try:
					srt.put(buf, msg)
					break
				except srt.QueueFullError:
					continue
	finally:
		shm.close()


def _consumer_mixed(shm_name, num_messages, ready):
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		ready.wait()
		sizes = [1, 1, 1, 3, 5, 10, 10, 50, 100, 200]
		for i in range(num_messages):
			size = sizes[i % len(sizes)]
			expected = bytes([i % 256] * size)
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


class TestMultiprocessing(unittest.TestCase):
	def _run_spsc(self, buf_size, num_messages):
		shm = sm.SharedMemory(create=True, size=buf_size)
		try:
			ready = multiprocessing.Event()

			p = multiprocessing.Process(
				target=_producer, args=(shm.name, num_messages, ready)
			)
			c = multiprocessing.Process(
				target=_consumer, args=(shm.name, num_messages, ready)
			)

			c.start()
			p.start()

			p.join(timeout=10)
			c.join(timeout=10)

			self.assertFalse(p.is_alive(), "Producer timed out")
			self.assertFalse(c.is_alive(), "Consumer timed out")
			self.assertEqual(p.exitcode, 0, f"Producer crashed: {p.exitcode}")
			self.assertEqual(c.exitcode, 0, f"Consumer failed: {c.exitcode}")
		finally:
			shm.close()
			shm.unlink()

	def test_spsc_stress_10k(self):
		self._run_spsc(buf_size=256, num_messages=10000)

	def test_spsc_stress_100k(self):
		self._run_spsc(buf_size=256, num_messages=100000)

	def test_spsc_stress_mixed_sizes(self):
		buf_size = 4096
		num_messages = 20000

		shm = sm.SharedMemory(create=True, size=buf_size)
		try:
			ready = multiprocessing.Event()

			c = multiprocessing.Process(
				target=_consumer_mixed, args=(shm.name, num_messages, ready)
			)
			p = multiprocessing.Process(
				target=_producer_mixed, args=(shm.name, num_messages, ready)
			)

			c.start()
			p.start()
			p.join(timeout=10)
			c.join(timeout=10)

			self.assertFalse(p.is_alive())
			self.assertFalse(c.is_alive())
			self.assertEqual(p.exitcode, 0)
			self.assertEqual(c.exitcode, 0)
		finally:
			shm.close()
			shm.unlink()
