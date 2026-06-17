import os
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class GenieClient:
    """
    Production Genie Conversation API client.

    One instance per Databricks App process (instantiated in app.py).
    Maintains one conversation_id per user session — follow-up questions
    share context. Call reset() or pass new_conversation=True to start fresh.

    Recommendations from Databricks docs implemented here:
    - Poll every 1–5 s (we use adaptive: start at 1s, cap at 5s)
    - Exponential backoff on transient HTTP errors
    - Hard 10-minute timeout per message
    - New conversation per user session (not shared across sessions)
    """

    TERMINAL      = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
    MAX_WAIT_S    = 600
    POLL_START_S  = 1.0
    POLL_MAX_S    = 5.0
    BACKOFF_BASE  = 2
    MAX_RETRIES   = 4

    def __init__(self, space_id: str):
        self.space_id = space_id
        self.host     = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        self.token    = os.environ.get("DATABRICKS_TOKEN", "")

        if not self.host or not self.token:
            raise EnvironmentError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN must be set. "
                "Inside a Databricks App these are injected automatically."
            )

        self.conversation_id: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type":  "application/json",
        })

    def _url(self, path: str) -> str:
        return f"{self.host}/api/2.0/genie/spaces/{self.space_id}/{path}"

    def _post(self, path: str, body: dict) -> dict:
        """POST with retry on 5xx / connection errors."""
        url = self._url(path)
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = self._session.post(url, json=body, timeout=20)
                if resp.status_code < 500:
                    resp.raise_for_status()
                    return resp.json()
                wait = self.BACKOFF_BASE ** attempt
                logger.warning(f"POST {url} → {resp.status_code}, retry in {wait}s")
                time.sleep(wait)
            except requests.ConnectionError as e:
                wait = self.BACKOFF_BASE ** attempt
                logger.warning(f"Connection error on POST {url}: {e}, retry in {wait}s")
                time.sleep(wait)
        raise RuntimeError(f"POST {path} failed after {self.MAX_RETRIES} retries")

    def _get(self, path: str) -> dict:
        """GET with retry on 5xx / connection errors."""
        url = self._url(path)
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = self._session.get(url, timeout=20)
                if resp.status_code < 500:
                    resp.raise_for_status()
                    return resp.json()
                wait = self.BACKOFF_BASE ** attempt
                logger.warning(f"GET {url} → {resp.status_code}, retry in {wait}s")
                time.sleep(wait)
            except requests.ConnectionError as e:
                wait = self.BACKOFF_BASE ** attempt
                logger.warning(f"Connection error on GET {url}: {e}, retry in {wait}s")
                time.sleep(wait)
        raise RuntimeError(f"GET {path} failed after {self.MAX_RETRIES} retries")

    def _start_conversation(self, question: str) -> tuple[str, str]:
        """
        POST /api/2.0/genie/spaces/{space_id}/start-conversation
        Returns (conversation_id, message_id).
        """
        body = self._post("start-conversation", {"content": question})
        conv_id = body.get("conversation_id") or body.get("id")
        msg_id  = body.get("message_id") or (body.get("messages", [{}])[0].get("id"))
        if not conv_id or not msg_id:
            raise RuntimeError(f"Unexpected start-conversation response: {body}")
        logger.info(f"Genie: new conversation {conv_id}, message {msg_id}")
        return conv_id, msg_id

    def _continue_conversation(self, question: str) -> str:
        """
        POST /api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages
        Returns message_id for the follow-up.
        """
        body   = self._post(f"conversations/{self.conversation_id}/messages", {"content": question})
        msg_id = body.get("id") or body.get("message_id")
        if not msg_id:
            raise RuntimeError(f"Unexpected create-message response: {body}")
        logger.info(f"Genie: follow-up message {msg_id} in conversation {self.conversation_id}")
        return msg_id

    def _poll(self, conversation_id: str, message_id: str) -> dict:
        """
        GET /api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}
        Polls with adaptive interval until a terminal status is reached.
        Returns the full message body.
        """
        path    = f"conversations/{conversation_id}/messages/{message_id}"
        elapsed = 0.0
        interval = self.POLL_START_S

        while elapsed < self.MAX_WAIT_S:
            body   = self._get(path)
            status = body.get("status", "")

            logger.debug(f"Genie poll: message {message_id} status={status} elapsed={elapsed:.1f}s")

            if status in self.TERMINAL:
                return body

            time.sleep(interval)
            elapsed  += interval
            interval  = min(interval * 1.5, self.POLL_MAX_S)

        raise RuntimeError(
            f"Genie timed out after {self.MAX_WAIT_S}s for message {message_id}. "
            "Check the Genie Space and SQL warehouse status."
        )

    def ask(self, question: str, new_conversation: bool = False) -> str:

        if self.conversation_id is None or new_conversation:
            conv_id, msg_id      = self._start_conversation(question)
            self.conversation_id = conv_id
        else:
            conv_id = self.conversation_id
            msg_id  = self._continue_conversation(question)

        message = self._poll(conv_id, msg_id)
        status  = message.get("status", "")

        if status == "FAILED":
            error = (message.get("error") or {}).get("message", "Unknown error")
            raise RuntimeError(f"Genie failed to answer: {error}")

        if status == "CANCELLED":
            raise RuntimeError("Genie message was cancelled.")

        if status == "QUERY_RESULT_EXPIRED":
            raise RuntimeError("Genie query result expired before it could be retrieved.")

        return self._extract_text(message)

    def reset(self) -> None:
        logger.info(f"Genie: resetting conversation {self.conversation_id}")
        self.conversation_id = None

    @staticmethod
    def _extract_text(message: dict) -> str:
        parts = []
        for att in message.get("attachments", []):
            # text attachment
            if "text" in att:
                content = att["text"].get("content", "")
                if content:
                    parts.append(content)
            # query attachment
            elif "query" in att:
                q = att["query"]
                desc = q.get("description", "")
                if desc:
                    parts.append(desc)
                row_count = (q.get("query_result_metadata") or {}).get("row_count")
                if row_count is not None:
                    parts.append(f"Results: {row_count} row(s) found.")

        return "\n\n".join(parts) if parts else message.get("content", "No response.")
