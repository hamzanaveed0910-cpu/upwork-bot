import json
import threading
from typing import List, Dict, Any


class ConfigManager:
    def __init__(self, path: str = 'config.json'):
        self.path = path
        self._lock = threading.RLock()
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            # default structure
            return {"tracked_urls": [], "fetch_interval": 30, "token_refresh_hours": 11}

    def _save(self) -> None:
        with self._lock:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)

    def get_tracked(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._data.get('tracked_urls', []))

    def get_config(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def set_tracked(self, tracked: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._data['tracked_urls'] = tracked
            self._save()

    def add_tracked(self, url: str, label: str, keyword: str, channel_id: int) -> None:
        with self._lock:
            entry = {"url": url, "label": label, "keyword": keyword, "channel_id": channel_id}
            self._data.setdefault('tracked_urls', []).append(entry)
            self._save()

    def remove_tracked_by_label(self, label: str) -> bool:
        with self._lock:
            tracked = self._data.get('tracked_urls', [])
            new = [t for t in tracked if t.get('label') != label]
            if len(new) == len(tracked):
                return False
            self._data['tracked_urls'] = new
            self._save()
            return True

    def remove_tracked_by_channel(self, channel_id: int) -> int:
        """Remove all tracked entries that reference the given channel_id.

        Returns the number of entries removed.
        """
        with self._lock:
            tracked = self._data.get('tracked_urls', [])
            new = [t for t in tracked if int(t.get('channel_id', 0)) != int(channel_id)]
            removed = len(tracked) - len(new)
            if removed:
                self._data['tracked_urls'] = new
                self._save()
            return removed

    def update_tracked(self, label: str, **fields) -> bool:
        with self._lock:
            for t in self._data.get('tracked_urls', []):
                if t.get('label') == label:
                    t.update(fields)
                    self._save()
                    return True
            return False
