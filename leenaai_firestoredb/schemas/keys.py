from dataclasses import dataclass


@dataclass
class FireStoreKeys:
    clientId = "client_id"
    updatedAt = "updated_at"
    DESCENDING = "DESCENDING"
    lastAction = "last_action"
    unreadMessagesCount = "unread_messages_count"
