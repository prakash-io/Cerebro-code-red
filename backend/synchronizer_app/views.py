import random
import string
import uuid

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Room, Listener
from .serializers import RoomSerializer


@api_view(["POST"])
def create_room(request):
    def generate_room_code(length: int = 6) -> str:
        alphabet = string.ascii_uppercase + string.digits
        return "".join(random.choices(alphabet, k=length))

    # Require broadcaster field per decision
    broadcaster = (request.data.get("broadcaster") or "").strip()
    if not broadcaster:
        return Response({"detail": "broadcaster is required."}, status=status.HTTP_400_BAD_REQUEST)

    video_url = request.data.get("video_url", "")

    # Accept optional room_code; generate if missing
    room_code = request.data.get("room_code")
    if not room_code:
        # try to generate a unique code a few times
        for _ in range(6):
            candidate = generate_room_code(6)
            if not Room.objects.filter(room_code=candidate).exists():
                room_code = candidate
                break
        if not room_code:
            return Response({"detail": "Could not generate unique room code"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Check if room already exists
    if Room.objects.filter(room_code=room_code).exists():
        return Response(
            {"detail": f"Room '{room_code}' already exists. Choose a different room code."},
            status=status.HTTP_409_CONFLICT
        )

    room = Room.objects.create(
        room_code=room_code,
        video_url=video_url,
        broadcaster=broadcaster,
    )

    return Response({"room_code": room.room_code}, status=status.HTTP_201_CREATED)


@api_view(["POST"])
def join_room(request):

    room_code = request.data.get("room_code")

    if not room_code:
        return Response({"error": "Room code required"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        room = Room.objects.get(room_code=room_code)
    except Room.DoesNotExist:
        return Response({"error": "Room not found"}, status=status.HTTP_404_NOT_FOUND)

    # Accept an optional client-provided listener_id; prefer a valid UUID from client
    client_listener_id = request.data.get("listener_id")
    listener = None
    if client_listener_id:
        try:
            lid = uuid.UUID(str(client_listener_id))
            # try to get or create listener with provided id
            listener, created = Listener.objects.get_or_create(room=room, listener_id=lid)
        except Exception:
            listener = None

    if not listener:
        listener = Listener.objects.create(room=room)

    return Response({
        "room_code": room.room_code,
        "listener_id": str(listener.listener_id)
    })


@api_view(["GET"])
def get_room_state(request, room_code):

    try:
        room = Room.objects.get(room_code=room_code)

    except Room.DoesNotExist:
        return Response({"error": "Room not found"}, status=404)
    serializer = RoomSerializer(room)
    return Response(serializer.data)
