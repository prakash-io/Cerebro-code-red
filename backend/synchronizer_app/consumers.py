import json
import time
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async


class SyncConsumer(AsyncWebsocketConsumer):

    # Shared across all instances: tracks pending room deletion tasks
    _pending_deletions = {}

    async def connect(self):
        self.room_code = self.scope["url_route"]["kwargs"]["room_code"]
        self.room_group = f"room_{self.room_code}"
        self.is_broadcaster = False
        
        # Last sync time for throttling
        self.last_sync_time = 0

        await self.channel_layer.group_add(
            self.room_group,
            self.channel_name,
        )
        await self.accept()

        # Increment listener count
        count = await self.modify_listener_count(1)
        await self.channel_layer.group_send(
            self.room_group,
            {
                "type": "sync_message",
                "message": {
                    "event": "listener_count",
                    "data": {"listener_count": count}
                },
            },
        )

    async def disconnect(self, close_code):
        # Decrement listener count
        count = await self.modify_listener_count(-1)
        await self.channel_layer.group_send(
            self.room_group,
            {
                "type": "sync_message",
                "message": {
                    "event": "listener_count",
                    "data": {"listener_count": count}
                },
            },
        )

        # If the broadcaster leaves, schedule room deletion after 2 minutes
        if self.is_broadcaster:
            self._schedule_room_deletion(self.room_code, self.room_group)

        await self.channel_layer.group_discard(
            self.room_group,
            self.channel_name,
        )

    async def receive(self, text_data):
        try:
            data = json.loads(text_data)
        except json.JSONDecodeError:
            return

        event_type = data.get("type", data.get("event"))

        # Track broadcaster identity
        if event_type == "identify" and data.get("role") == "broadcaster":
            self.is_broadcaster = True
            # Cancel any pending deletion (broadcaster rejoined in time)
            self._cancel_pending_deletion(self.room_code)
            return

        # Prevent spam: throttle sync_state to once every 2 seconds
        # Other critical events (play, pause, webrtc, load_video, broadcast_message) bypass throttling.
        if event_type == "sync_state":
            now = time.time()
            if now - self.last_sync_time < 2.0:
                return
            self.last_sync_time = now

        # Broadcast the message to the group
        await self.channel_layer.group_send(
            self.room_group,
            {
                "type": "sync_message",
                "message": data,
            },
        )

    async def sync_message(self, event):
        await self.send(
            text_data=json.dumps(event["message"])
        )

    # ── Delayed room deletion (2-minute grace period) ──

    @classmethod
    def _schedule_room_deletion(cls, room_code, room_group):
        """Schedule room deletion after 120 seconds."""
        # Cancel any existing timer for this room first
        cls._cancel_pending_deletion(room_code)

        async def _delayed_delete():
            await asyncio.sleep(120)  # 2-minute grace period
            from channels.db import database_sync_to_async as db_async
            from .models import Room

            @db_async
            def do_delete():
                Room.objects.filter(room_code=room_code).delete()

            await do_delete()
            cls._pending_deletions.pop(room_code, None)

        task = asyncio.ensure_future(_delayed_delete())
        cls._pending_deletions[room_code] = task

    @classmethod
    def _cancel_pending_deletion(cls, room_code):
        """Cancel a pending room deletion if broadcaster rejoins."""
        task = cls._pending_deletions.pop(room_code, None)
        if task and not task.done():
            task.cancel()

    # ── Database helpers ──

    @database_sync_to_async
    def modify_listener_count(self, delta):
        from .models import Room
        try:
            room = Room.objects.get(room_code=self.room_code)
            room.listener_count = max(0, room.listener_count + delta)
            room.save(update_fields=['listener_count'])
            return room.listener_count
        except Room.DoesNotExist:
            return 0
