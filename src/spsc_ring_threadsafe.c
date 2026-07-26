#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdatomic.h>
#include <stddef.h>



#ifdef __GNUC__
	_Static_assert(__atomic_always_lock_free(sizeof(_Atomic uint32_t), 0), "32-bit atomics not lock-free");
	_Static_assert(__atomic_always_lock_free(sizeof(_Atomic uint64_t), 0), "64-bit atomics not lock-free");
#else
	_Static_assert(ATOMIC_INT_LOCK_FREE == 2, "32-bit atomics not lock-free");
	_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "64-bit atomics not lock-free");
#endif

#define SRT_MSG_PREFIX_SIZE 4
#define SRT_BUF_SIZE_MIN ((size_t)256)
#define SRT_BUF_SIZE_MAX ((size_t)(1 << 31))
#define SRT_CACHE_LINE_ALIGNMENT 64
#define SRT_CACHE_LINE_ALIGNMENT_MASK ((size_t)(SRT_CACHE_LINE_ALIGNMENT - 1))

_Static_assert(SRT_BUF_SIZE_MIN >= 4 * SRT_CACHE_LINE_ALIGNMENT);

typedef struct {
	_Alignas(64) _Atomic uint32_t read;
	char pad1[SRT_CACHE_LINE_ALIGNMENT - sizeof(_Atomic uint32_t)];
	_Alignas(64) _Atomic uint64_t write_wm; /* upper 32: write, lower 32: watermark*/
	char pad2[SRT_CACHE_LINE_ALIGNMENT - sizeof(_Atomic uint64_t)];
} srt_header;

_Static_assert(sizeof(srt_header) == 2 * SRT_CACHE_LINE_ALIGNMENT);
_Static_assert(offsetof(srt_header, write_wm) == SRT_CACHE_LINE_ALIGNMENT);

#define SRT_HEADER_WRITE_WM_WATERMARK_MASK 0xFFFFFFFFULL
#define SRT_HEADER_WRITE_WM_WRITE_SHIFT 32



static PyObject *srt_QueueEmptyError;
static PyObject *srt_QueueFullError;

static int srt_exec(PyObject *module)
{
	srt_QueueEmptyError = PyErr_NewExceptionWithDoc(
		"spsc_ring_threadsafe.QueueEmptyError",
		"cannot get item: buffer empty",
		NULL, NULL);
	if (PyModule_AddObjectRef(module, "QueueEmptyError", srt_QueueEmptyError) < 0)
		goto cleanup;
	srt_QueueFullError = PyErr_NewExceptionWithDoc(
		"spsc_ring_threadsafe.QueueFullError",
		"cannot put item: buffer full",
		NULL, NULL);
	if (PyModule_AddObjectRef(module, "QueueFullError", srt_QueueFullError) < 0)
		goto cleanup;
	return 0;
cleanup:
	Py_CLEAR(srt_QueueEmptyError);
	Py_CLEAR(srt_QueueFullError);
	return -1;
}

static inline size_t _srt_get_wm_size_min(unsigned char *buf, size_t bufsz)
{
	uintptr_t buf_end = (uintptr_t)buf + bufsz;
	uintptr_t partial = buf_end & SRT_CACHE_LINE_ALIGNMENT_MASK;
	return 2 * SRT_CACHE_LINE_ALIGNMENT + partial;
}

static inline int _srt_buf_validate(unsigned char *buf, size_t bufsz)
{
	(void)buf;
	if (bufsz < SRT_BUF_SIZE_MIN) {
		PyErr_Format(PyExc_ValueError, "init() buffer must have minimum size %zu", SRT_BUF_SIZE_MIN);
		return -1;
	}
	if (bufsz > SRT_BUF_SIZE_MAX) {
		PyErr_Format(PyExc_ValueError, "init() buffer must have maximum size %zu", SRT_BUF_SIZE_MAX);
		return -1;
	}
	if (!(bufsz > 0 && (bufsz & (bufsz - 1)) == 0)) {
		PyErr_Format(PyExc_ValueError, "init() buffer size must be a power of two");
		return -1;
	}
	return 0;
}

static PyObject *srt_init(PyObject *self, PyObject *buf_obj)
{
	(void)self;
	Py_buffer pybuf;
	if (PyObject_GetBuffer(buf_obj, &pybuf, PyBUF_SIMPLE | PyBUF_WRITABLE) < 0)
		return NULL;
	unsigned char *buf = (unsigned char *)pybuf.buf;
	size_t bufsz = (size_t)pybuf.len;
	if (_srt_buf_validate(buf, bufsz) < 0)
		goto cleanup;
	size_t wm_size_min = _srt_get_wm_size_min(buf, bufsz);
	srt_header *h = (srt_header *)(buf + bufsz - wm_size_min);
	atomic_store_explicit(&h->read, 0, memory_order_relaxed);
	atomic_store_explicit(&h->write_wm, 0, memory_order_release);
	PyBuffer_Release(&pybuf);
	Py_RETURN_NONE;
cleanup:
	PyBuffer_Release(&pybuf);
	return NULL;
}

static inline int _srt_write(
	unsigned char *restrict buf,
	size_t bufsz,
	unsigned char *restrict data,
	size_t data_len)
{
	size_t wm_size_min = _srt_get_wm_size_min(buf, bufsz);
	srt_header *h = (srt_header *)(buf + bufsz - wm_size_min);

	uint64_t write_wm = atomic_load_explicit(&h->write_wm, memory_order_relaxed);
	uint32_t write = (uint32_t)(write_wm >> SRT_HEADER_WRITE_WM_WRITE_SHIFT);
	size_t wm = write_wm & SRT_HEADER_WRITE_WM_WATERMARK_MASK;

	uint32_t read = atomic_load_explicit(&h->read, memory_order_acquire);

	size_t mask = bufsz - 1;
	size_t write_ofs = write & mask;
	size_t read_ofs = read & mask;

	if (data_len > bufsz
		|| write_ofs > bufsz - data_len
		|| wm_size_min > bufsz - data_len - write_ofs
		|| SRT_MSG_PREFIX_SIZE > bufsz - data_len - write_ofs - wm_size_min) {
		wm = bufsz - write_ofs;
		write += (uint32_t)wm;
	}
	size_t written = write - read;
	if (written > bufsz
		|| data_len > bufsz - written
		|| SRT_MSG_PREFIX_SIZE > bufsz - written - data_len) {
		PyErr_SetNone(srt_QueueFullError);
		return -1;
	}
	memcpy(buf + (write & mask), &data_len, SRT_MSG_PREFIX_SIZE);
	write += SRT_MSG_PREFIX_SIZE;
	memcpy(buf + (write & mask), data, data_len);
	write += (uint32_t)data_len;
	atomic_store_explicit(
		&h->write_wm,
		((uint64_t)write << SRT_HEADER_WRITE_WM_WRITE_SHIFT) | (wm & SRT_HEADER_WRITE_WM_WATERMARK_MASK),
		memory_order_release
	);
	return 0;
}

static inline int _srt_read(
	unsigned char *restrict buf,
	size_t bufsz,
	PyObject **out_obj)
{
	size_t wm_size_min = _srt_get_wm_size_min(buf, bufsz);
	srt_header *h = (srt_header *)(buf + bufsz - wm_size_min);

	uint64_t write_wm = atomic_load_explicit(&h->write_wm, memory_order_acquire);
	uint32_t write = (uint32_t)(write_wm >> SRT_HEADER_WRITE_WM_WRITE_SHIFT);
	size_t wm = write_wm & SRT_HEADER_WRITE_WM_WATERMARK_MASK;

	uint32_t read = atomic_load_explicit(&h->read, memory_order_relaxed);

	size_t mask = bufsz - 1;
	size_t write_ofs = write & mask;
	size_t read_ofs = read & mask;

	if (write - read > bufsz - read_ofs && read_ofs == bufsz - wm) {
		read += (uint32_t)wm;
	}
	size_t written = write - read;
	if (written == 0) {
		PyErr_SetNone(srt_QueueEmptyError);
		return -1;
	}
	size_t out_len = 0;
	memcpy(&out_len, buf + (read & mask), SRT_MSG_PREFIX_SIZE);
	read += SRT_MSG_PREFIX_SIZE;
	if ((*out_obj = PyBytes_FromStringAndSize(
		(const char *)(buf + (read & mask)), (Py_ssize_t)out_len)) == NULL)
		return -1;
	read += (uint32_t)out_len;
	atomic_store_explicit(&h->read, read, memory_order_release);
	return 0;
}

static PyObject *srt_put(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
	(void)self;
	if (nargs != 2) {
		PyErr_Format(PyExc_TypeError, "put() takes exactly 2 arguments (%zd given)", nargs);
		return NULL;
	}
	PyObject *restrict buf_obj = args[0];
	PyObject *restrict item_obj = args[1];
	Py_buffer pybuf;
	Py_buffer item_pybuf;
	if (PyObject_GetBuffer(buf_obj, &pybuf, PyBUF_SIMPLE | PyBUF_WRITABLE) < 0)
		return NULL;
	if (PyObject_GetBuffer(item_obj, &item_pybuf, PyBUF_SIMPLE) < 0) {
		PyBuffer_Release(&pybuf);
		return NULL;
	}
	if (_srt_buf_validate((unsigned char *)pybuf.buf, (size_t)pybuf.len) < 0)
		goto cleanup;
	if (_srt_write(	(unsigned char *)pybuf.buf,	(size_t)pybuf.len,
			(unsigned char *)item_pybuf.buf,(size_t)item_pybuf.len) < 0)
		goto cleanup;
	PyBuffer_Release(&pybuf);
	PyBuffer_Release(&item_pybuf);
	Py_RETURN_NONE;
cleanup:
	PyBuffer_Release(&pybuf);
	PyBuffer_Release(&item_pybuf);
	return NULL;
}

static PyObject *srt_get(PyObject *self, PyObject *buf_obj)
{
	(void)self;
	Py_buffer pybuf;
	if (PyObject_GetBuffer(buf_obj, &pybuf, PyBUF_SIMPLE | PyBUF_WRITABLE) < 0)
		return NULL;
	if (_srt_buf_validate((unsigned char *)pybuf.buf, (size_t)pybuf.len) < 0)
		goto cleanup;
	PyObject *ret = NULL;
	if (_srt_read((unsigned char *)pybuf.buf, (size_t)pybuf.len, &ret) < 0)
		goto cleanup;
	PyBuffer_Release(&pybuf);
	return ret;
cleanup:
	PyBuffer_Release(&pybuf);
	return NULL;
}

static PyMethodDef srt_methods[] = {
	{"put",		(PyCFunction)srt_put,	METH_FASTCALL,	"put(buf, item): Add an element"},
	{"get",		(PyCFunction)srt_get,	METH_O,	"get(buf): Remove and return an element"},
	{"init",	(PyCFunction)srt_init,	METH_O,	"init(buf): Initialize a raw buffer before use (required)"},
	{NULL, NULL, 0, NULL},
};

static PyModuleDef_Slot srt_slots[] = {
	{Py_mod_exec, srt_exec},
	{Py_mod_gil, Py_MOD_GIL_NOT_USED},
	{Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
	{0, NULL},
};

static struct PyModuleDef srt_module = {
	.m_base = PyModuleDef_HEAD_INIT,
	.m_name = "spsc_ring_threadsafe",
	.m_doc = "Thread-safe SPSC queue for Python, implemented on a ring buffer written in C\n",
	.m_size = 0,
	.m_methods = srt_methods,
	.m_slots = srt_slots,
	.m_traverse = NULL,
	.m_clear = NULL,
	.m_free = NULL,
};

PyMODINIT_FUNC PyInit_spsc_ring_threadsafe(void)
{
	return PyModuleDef_Init(&srt_module);
}