from typing import List
import asyncio
import logging

class Broadcaster:
    def __init__(self):
        self._connections: List[asyncio.Queue] = []

    def register(self) -> asyncio.Queue:
        q = asyncio.Queue()
        self._connections.append(q)
        return q

    def unregister(self, q: asyncio.Queue):
        try:
            self._connections.remove(q)
        except ValueError:
            pass

    async def broadcast(self, message: dict):
        for q in list(self._connections):
            try:
                await q.put(message)
            except Exception:
                logging.exception("Failed to put message to queue")


_b = Broadcaster()


def notify_event(payload: dict):
    """Synchronous convenience wrapper to schedule an async broadcast."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_b.broadcast(payload))
        else:
            loop.run_until_complete(_b.broadcast(payload))
    except RuntimeError:
        # no running event loop — fire and forget in background
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_b.broadcast(payload))


async def websocket_endpoint(websocket):
    q = _b.register()
    try:
        await websocket.accept()
        while True:
            # create tasks for both coroutines and wait for the first to complete
            recv_task = asyncio.create_task(websocket.receive_text())
            q_task = asyncio.create_task(q.get())

            done, pending = await asyncio.wait(
                {recv_task, q_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            # handle queue message
            if q_task in done:
                try:
                    res = q_task.result()
                    if isinstance(res, dict):
                        await websocket.send_json(res)
                except Exception:
                    logging.exception("Error sending websocket message")

            # handle client message (keep-alive/ping)
            if recv_task in done:
                try:
                    _ = recv_task.result()
                    # ignore client text
                except Exception:
                    # client likely disconnected
                    for p in pending:
                        p.cancel()
                    break

            # cancel any pending task to avoid 'coroutine was never awaited'
            for p in pending:
                p.cancel()
                try:
                    await p
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        _b.unregister(q)
