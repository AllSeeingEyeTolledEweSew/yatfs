import asyncio
import concurrent
import functools
import threading

class Routines(object):

    def __init__(self, io_workers=128, loop=None):
        self.io_workers = io_workers
        self.loop = loop or asyncio.new_event_loop()

        self._io_pool = None

    @property
    def io_pool(self):
        if not self._io_pool:
            self._io_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.io_workers)
        return self._io_pool

    @asyncio.coroutine
    def call_io_async(self, func, *args, **kwargs):
        return (yield from self.loop.run_in_executor(
            self.io_pool, functools.partial(func, *args, **kwargs)))

    def call_in_loop(self, coro_or_future):
        event = threading.Event()
        task_h = [None]

        def schedule():
            task_h[0] = self.loop.create_task(coro_or_future)

            def done_cb(fut):
                event.set()

            task_h[0].add_done_callback(done_cb)

        self.loop.call_soon_threadsafe(schedule)
        event.wait()
        return task_h[0].result()

    def run_loop_in_background(self):
        thread = threading.Thread(name="loop", target=self.loop.run_forever)
        thread.start()

    def stop_loop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
