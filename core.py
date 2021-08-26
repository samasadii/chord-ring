import websockets
import json
import random
import asyncio


class Core:
    max_nodes = 32

    def __init__(self, address, port, id):
        self.id = id
        self.address = address
        self.port = port
        self.ft = []
        self.me = {'address': address, 'port': port, 'id': id}

        self.next = {'address': address, 'port': port, 'id': id}
        self.back = {'address': address, 'port': port, 'id': id}

    async def get_id(self):
        return self.id

    async def find_proper_chord(self, id_to_search):

        if id_to_search in await self.keys():
            return self.me
        else:
            ft_record = await self.get_proper_ft_record(id_to_search)
            if ft_record['id'] == self.id:
                dispatch_node = self.next
            else:
                dispatch_node = ft_record

            return await self.remote_call(dispatch_node, 'find_proper_chord', {'id_to_search': id_to_search})

    @staticmethod
    async def remote_call(remote_address, call_function, arguments=None):
        if arguments is None:
            arguments = {}
        websocket = await websockets.connect("ws://%s:%d" % (remote_address['address'], remote_address['port']))
        await websocket.send(json.dumps({'call_function': call_function, 'arguments': arguments}))
        raw_response = await websocket.recv()
        return json.loads(raw_response) if raw_response is not None else None

    async def get_proper_ft_record(self, lookup_key):
        for i in range(len(self.ft) - 1):
            if self.ft[i]['id'] <= lookup_key <= self.ft[i + 1]['id']:
                return self.ft[i]
        max_record = max(self.ft, key=lambda record: record['id'])
        return max_record

    async def is_mine(self, lookup_key):
        return lookup_key in await self.keys()

    async def guess_and_find(self):
        candidate_id = random.randint(0, self.max_nodes - 1)
        next_node_in_ring = await self.find_proper_chord(candidate_id)

        return candidate_id, next_node_in_ring

    async def insert(self, remote_address):

        new_id = self.id
        next_node = await self.remote_call(remote_address, 'find_proper_chord', {'id_to_search': new_id})
        back_node = await self.remote_call(next_node, 'get_prev')

        self.id = new_id
        self.me['id'] = new_id
        self.next = next_node
        self.back = back_node

        await self.remote_call(next_node, 'set_prev', {'prev': self.me})
        await self.remote_call(back_node, 'set_succ', {'succ': self.me})

        await self.update_ft_and_others(self.id)

        print("[ %d ]" % self.id, "Connected To Ring")

    async def ray_node_find(self, id_to_search):
        if await self.is_mine(id_to_search):
            return self.me
        return await self.remote_call(self.next, "ray_node_find", {'id_to_search': id_to_search})

    async def create_ft(self):
        self.ft = []
        for i in range(5):
            ft_record = (self.id + 2 ** i) % self.max_nodes
            _node = await self.ray_node_find(ft_record)
            self.ft.append(_node)

    async def get_ft(self):
        return self.ft

    async def update_ft_and_others(self, node):
        await self.create_ft()
        if self.next['id'] != node:
            await self.remote_call(self.next, 'update_ft_and_others', {'node': node})

    async def keys(self):
        back_id = self.back['id']
        my_id = self.id

        return list(range(back_id + 1, my_id + 1)) if my_id > back_id else \
            list(range(back_id + 1, self.max_nodes)) + list(range(0, my_id + 1)) if my_id < back_id else \
                list(range(0, self.max_nodes))

    async def get_prev(self):
        return self.back

    async def get_succ(self):
        return self.next

    async def set_prev(self, prev):
        print("[ %d ]" % self.id, "setting prev to ", prev)
        self.back = prev

        print(self.back)

    async def set_succ(self, succ):
        self.next = succ

    async def entry(self, websocket, path):
        while True:
            command = await websocket.recv()
            parsed_command = json.loads(command)

            call_function = parsed_command['call_function']
            arguments = parsed_command['arguments']

            actual_function = eval("self." + call_function)
            res = await actual_function(**arguments)
            json_res = json.dumps(res)
            await websocket.send(json_res)
            return json_res

    def setup(self):
        el = asyncio.new_event_loop()
        asyncio.set_event_loop(el)

        server = websockets.serve(self.entry, self.address, self.port)
        asyncio.get_event_loop().run_until_complete(server)
        print("[ %d ] " % self.id, "Server Up and Running")
        asyncio.get_event_loop().run_forever()
