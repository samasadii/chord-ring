from core import Core
import asyncio
from threading import Thread
import time

_run = asyncio.get_event_loop().run_until_complete


def chord_runner(chord):
    chord.setup()


def start_chord(chord):
    _t = Thread(target=chord_runner, args=(chord,))
    _t.start()
    _threads.append(_t)
    time.sleep(1)


def create_and_connect_to_ring(id, address, port, chord_ring_info):
    _chord = Core(address, port, id)
    start_chord(_chord)
    _chord_info = _chord.me
    _run(Core.remote_call(_chord_info, 'insert', {'remote_address': chord_ring_info}))

    return _chord_info


def __run(chord_info, func, kwargs):
    return _run(Core.remote_call(chord_info, func, kwargs))


if __name__ == "__main__":

    _threads = []

    _chord = Core("localhost", 1212, 1)
    start_chord(_chord)

    _chord_info = _chord.me

    print("[client] Get Id (1): ", _run(Core.remote_call(_chord_info, 'get_id')))
    print("[client] Get FT(1): ", _run(Core.remote_call(_chord_info, 'get_ft')))
    _run(Core.remote_call(_chord_info, 'create_ft'))
    print("[client] Find 5 in node 1", _run(Core.remote_call(_chord_info, 'find_proper_chord', {'id_to_search': 5})))

    c4_info = create_and_connect_to_ring(4, "localhost", 2020, _chord_info)
    c25_info = create_and_connect_to_ring(25, "localhost", 5656, _chord_info)

    print("[client] Get Prev(1): ", _run(Core.remote_call(_chord_info, 'get_prev')))
    print("[client] Get Succ(1): ", _run(Core.remote_call(_chord_info, 'get_succ')))
    print("[client] Get Succ(1): ", _run(Core.remote_call(_chord_info, 'keys')))
    print("[client] Get Succ(1): ", _run(Core.remote_call(_chord_info, 'get_ft')))
    print("-----------------------------------------------------------------------------")

    print("[client] Get Prev(4): ", _run(Core.remote_call(c4_info, 'get_prev')))
    print("[client] Get Succ(4): ", _run(Core.remote_call(c4_info, 'get_succ')))
    print("[client] Get Succ(4): ", _run(Core.remote_call(c4_info, 'keys')))
    print("[client] Get Succ(4): ", _run(Core.remote_call(c4_info, 'get_ft')))
    print("-----------------------------------------------------------------------------")

    print("[client] Get Prev(25): ", _run(Core.remote_call(c25_info, 'get_prev')))
    print("[client] Get Succ(25: ", _run(Core.remote_call(c25_info, 'get_succ')))
    print("[client] Get Succ(25: ", _run(Core.remote_call(c25_info, 'keys')))
    print("[client] Get Succ(25: ", _run(Core.remote_call(c25_info, 'get_ft')))
    print("-----------------------------------------------------------------------------")
    #
    # print("[client] ---------------------------------------------------------")
    #
    print("[Client ]", _run(Core.remote_call(c4_info, "find_proper_chord", {'id_to_search': 12})))
    print("[Client ]", _run(Core.remote_call(c4_info, "find_proper_chord", {'id_to_search': 12})))
    print("[Client ]", _run(Core.remote_call(c4_info, "find_proper_chord", {'id_to_search': 1})))

    for t in _threads:
        t.join()
