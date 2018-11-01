# Copyright 2018 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Class for websocket client between citizen node and mother peer"""

import asyncio
import json
import logging

import websockets
from websockets.exceptions import InvalidStatusCode

from loopchain import configure as conf
from loopchain.baseservice import TimerService, Timer
from loopchain.blockchain import Block
from loopchain.protos import message_code


class WebSocketClient:
    def __init__(self, channel, peer_target, rs_target, block_height, channel_service):
        self.__channel = channel
        self.__peer_target = peer_target
        self.__block_height = block_height
        self.__rs_target = rs_target
        self.__channel_service: 'ChannelService' = channel_service
        self.__target_uri = f"ws://{self.__rs_target}/api/node/{channel}"

    @property
    def block_height(self):
        return self.__block_height

    @block_height.setter
    def block_height(self, block_height):
        self.__block_height = block_height

    @property
    def target_uri(self):
        return self.__target_uri

    async def subscribe(self, uri):
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    await self.__stop_shutdown_timer()
                    request = json.dumps({
                        'height': self.__block_height
                    })
                    await websocket.send(request)

                    while True:
                        new_block_json = await websocket.recv()
                        response_code = await self.__add_confirmed_block(block_json=new_block_json)
                        if response_code == message_code.Response.success:
                            self.__block_height += 1
                        else:
                            raise RuntimeError
            except InvalidStatusCode as e:
                logging.warning(f"websocket subscribe InvalidStatusCode exception, caused by: {e}")
                raise NotImplementedError
            except Exception as e:
                logging.error(f"websocket subscribe exception, caused by: {e}")
                await self.__start_shutdown_timer()
                await asyncio.sleep(conf.SUBSCRIBE_RETRY_TIMER)

    async def __start_shutdown_timer(self):
        timer_key = TimerService.TIMER_KEY_SHUTDOWN_WHEN_FAIL_SUBSCRIBE
        if timer_key not in self.__channel_service.timer_service.timer_list.keys():
            error = f"Shutdown by Subscribe retry timeout({conf.SHUTDOWN_TIMER} sec)"
            self.__channel_service.timer_service.add_timer(
                timer_key,
                Timer(
                    target=timer_key,
                    duration=conf.SHUTDOWN_TIMER,
                    callback=self.__shutdown_peer,
                    callback_kwargs={"message": error}
                )
            )

    async def __stop_shutdown_timer(self):
        timer_key = TimerService.TIMER_KEY_SHUTDOWN_WHEN_FAIL_SUBSCRIBE
        if timer_key in self.__channel_service.timer_service.timer_list.keys():
            self.__channel_service.timer_service.stop_timer(timer_key)

    async def __add_confirmed_block(self, block_json: str):
        try:
            block_dict = json.loads(block_json)
            confirmed_block = Block(channel_name=self.__channel)
            confirmed_block.deserialize_block(block_json.encode('utf-8'))
            confirmed_block.commit_state = block_dict.get('commit_state')

            if self.__channel_service.block_manager.get_blockchain().block_height < confirmed_block.height:
                logging.debug(f"add_confirmed_block height({confirmed_block.height}), "
                              f"hash({confirmed_block.block_hash})")
                self.__channel_service.block_manager.add_confirmed_block(confirmed_block)

            response_code = message_code.Response.success
        except Exception as e:
            logging.error(f"websocket:__add_confirmed_block error : {e}")
            response_code = message_code.Response.fail
        return response_code

    def __shutdown_peer(self, **kwargs):
        self.__channel_service.shutdown_peer(**kwargs)
