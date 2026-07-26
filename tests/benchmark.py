import os
import multiprocessing
import multiprocessing.shared_memory as sm
import time
import math
import spsc_ring_threadsafe as srt


# DATA_TARGET = 4 * 1024 * 1024 * 1024  # 4 GiB
DATA_TARGET = 1 * 1024 * 1024 * 1024  # 1 GiB
# DATA_TARGET = 200 * 1024 * 1024 # 200 MiB

MSG_SIZES = [8 << i for i in range(22)]
BUF_SIZES = [4096 << i for i in range(20)]


def _detect_sibling_pairs():
   # Return list of (core, sibling) tuples from sysfs thread_siblings_list
	cpu_dir = "/sys/devices/system/cpu"
	if not os.path.exists(cpu_dir):
		return None
	seen = set()
	pairs = []
	for entry in sorted(os.listdir(cpu_dir)):
		if not entry.startswith("cpu") or not entry[3:].isdigit():
			continue
		cpu_id = int(entry[3:])
		sibling_file = f"{cpu_dir}/{entry}/topology/thread_siblings_list"
		if not os.path.exists(sibling_file):
			continue
		with open(sibling_file) as f:
			s = f.read().strip()
		if "-" in s:
			a, b = map(int, s.split("-"))
			sibling_set = frozenset(range(a, b + 1))
		else:
			sibling_set = frozenset(int(x) for x in s.split(","))
		if sibling_set not in seen:
			seen.add(sibling_set)
			pairs.append(tuple(sorted(sibling_set)))
	return pairs

sibling_pairs = _detect_sibling_pairs()

# Pinning eliminates scheduling variability, especially on NUMA-systems
def _pin_to_pair():
	if sibling_pairs is None:
		return
	pair = sibling_pairs[0]
	os.sched_setaffinity(0, set(pair))


def _spsc_producer(shm_name, msg_size, num_messages, ready):
	_pin_to_pair()
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		srt.init(buf)
		msg = bytes(msg_size)
		ready.set()
		for _ in range(num_messages):
			while True:
				try:
					srt.put(buf, msg)
					break
				except srt.QueueFullError:
					continue
	finally:
		shm.close()


def _spsc_consumer(shm_name, msg_size, num_messages, ready, result):
	_pin_to_pair()
	shm = sm.SharedMemory(name=shm_name)
	try:
		buf = shm.buf
		ready.wait()
		start = time.perf_counter()
		for _ in range(num_messages):
			while True:
				try:
					srt.get(buf)
					break
				except srt.QueueEmptyError:
					continue
		result.value = time.perf_counter() - start
	finally:
		shm.close()


def _queue_producer(q, msg_size, num_messages, ready):
	_pin_to_pair()
	msg = bytes(msg_size)
	ready.set()
	for _ in range(num_messages):
		while True:
			try:
				q.put_nowait(msg)
				break
			except Exception:
				continue


def _queue_consumer(q, num_messages, ready, result):
	_pin_to_pair()
	ready.wait()
	start = time.perf_counter()
	for _ in range(num_messages):
		while True:
			try:
				q.get_nowait()
				break
			except Exception:
				continue
	result.value = time.perf_counter() - start


def bench_spsc(buf_size, msg_size):
	num_messages = (DATA_TARGET + msg_size - 1) // msg_size
	shm = sm.SharedMemory(create=True, size=buf_size)
	result = multiprocessing.Value('d', 0.0)
	ready = multiprocessing.Event()
	try:
		p = multiprocessing.Process(
			target=_spsc_producer,
			args=(shm.name, msg_size, num_messages, ready),
		)
		c = multiprocessing.Process(
			target=_spsc_consumer,
			args=(shm.name, msg_size, num_messages, ready, result),
		)
		c.start()
		p.start()
		p.join(timeout=3600)
		c.join(timeout=3600)
		return result.value
	finally:
		shm.close()
		shm.unlink()


def bench_queue(msg_size):
	num_messages = (DATA_TARGET + msg_size - 1) // msg_size
	q = multiprocessing.Queue()
	result = multiprocessing.Value('d', 0.0)
	ready = multiprocessing.Event()
	p = multiprocessing.Process(
		target=_queue_producer, args=(q, msg_size, num_messages, ready)
	)
	c = multiprocessing.Process(
		target=_queue_consumer, args=(q, num_messages, ready, result)
	)
	c.start()
	p.start()
	p.join(timeout=3600)
	c.join(timeout=3600)
	return result.value


def run_matrix():
	queue_times = {}
	print("Benchmarking multiprocessing.Queue...")
	for msg_size in MSG_SIZES:
		t = bench_queue(msg_size)
		queue_times[msg_size] = t
		print(f"  msg={msg_size}: {t:.4f}s")

	results = {}
	print("\nBenchmarking spsc-ring-threadsafe...")
	for buf_size in BUF_SIZES:
		row = {}
		for msg_size in MSG_SIZES:
			if msg_size > buf_size // 4:
				row[msg_size] = None
				continue
			t = bench_spsc(buf_size, msg_size)
			row[msg_size] = t
			speedup = queue_times[msg_size] / t
			print(f"  buf={buf_size} msg={msg_size}: {t:.4f}s ({speedup:.2f}x)")
		results[buf_size] = row

	return queue_times, results


def generate_html(queue_times, results):
	half = len(MSG_SIZES) // 2
	cols_small = MSG_SIZES[:half]
	cols_large = MSG_SIZES[half:]

	def fmt_size(n):
		for unit, label in [
			(1024 * 1024 * 1024, "GB"),
			(1024 * 1024, "MB"),
			(1024, "KB"),
		]:
			if n >= unit:
				return f"{n // unit}{label}"
		return f"{n}B"

	def cell_html(buf_size, msg_size):
		spsc_t = results[buf_size].get(msg_size)
		if spsc_t is None:
			return '<td class="na">N/A</td>'
		q_t = queue_times[msg_size]
		ratio = q_t / spsc_t
		log_r = math.log2(ratio)
		if log_r >= 0:
			intensity = min(log_r / 5, 1.0)
			bg = f"rgba(0, 160, 0, {intensity * 0.92})"
		else:
			intensity = min(-log_r / 5, 1.0)
			bg = f"rgba(200, 0, 0, {intensity * 0.92})"
		fg = "white" if intensity > 0.55 else "black"
		return f'<td style="background:{bg};color:{fg}">{ratio:.2f}x</td>'

	def make_table(cols):
		lines = ['<table>', '<thead><tr><th>buf\\msg</th>']
		for msg_size in cols:
			lines.append(f"<th>{fmt_size(msg_size)}</th>")
		lines.append("</tr></thead><tbody>")
		for buf_size in BUF_SIZES:
			lines.append(f"<tr><th>{fmt_size(buf_size)}</th>")
			for msg_size in cols:
				lines.append(cell_html(buf_size, msg_size))
			lines.append("</tr>")
		lines.append("</tbody></table>")
		return "\n".join(lines)

	html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SPSC vs multiprocessing.Queue Benchmark</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2em; background: #f5f5f5; }}
h1 {{ font-size: 1.2em; margin-bottom: 0.5em; }}
h2 {{ font-size: 1em; margin-top: 2em; margin-bottom: 0.5em; }}
table {{ border-collapse: collapse; font-size: 0.75em; background: white; }}
th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: center; }}
th {{ background: #e0e0e0; font-weight: 600; }}
td {{ min-width: 4.5em; }}
td.na {{ background: #eee; color: #999; }}
</style>
</head>
<body>
<h1>SPSC Ring Buffer vs multiprocessing.Queue</h1>
<p>Values: speedup factor (higher is better for spsc-ring-threadsafe).<br>
Green = faster. Red = slower. N/A = message too large for buffer.<br>
Fixed data per cell: ~1 GiB.</p>
<h2>Small Message Sizes</h2>
{make_table(cols_small)}
<h2>Large Message Sizes</h2>
{make_table(cols_large)}
</body>
</html>"""
	return html


if __name__ == "__main__":
	queue_times, results = run_matrix()
	html = generate_html(queue_times, results)
	with open("benchmark.html", "w") as f:
		f.write(html)
	print("\nWrote benchmark.html")