import httpx
import asyncio
import time
from typing import Optional, Dict, Any
from app.config import settings


def _normalize_phone_for_api(phone: str) -> str:
    """Normalize phone for API: exactly 91 + 10 digits (India). Scoreplex uses this to return IN/India and correct carrier (e.g. JIO)."""
    if not phone and phone != 0:
        return str(phone) if phone == 0 else ""
    if isinstance(phone, float):
        phone = str(int(phone))
    elif isinstance(phone, int):
        phone = str(phone)
    else:
        phone = str(phone)
    phone_clean = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone_clean.startswith("+"):
        phone_clean = phone_clean[1:]
    if phone_clean.endswith(".0"):
        phone_clean = phone_clean[:-2]
    digits = "".join(c for c in phone_clean if c.isdigit())
    if not digits:
        return phone_clean
    # Always produce exactly 12 digits: 91 (India) + 10-digit number
    if digits.startswith("91"):
        if len(digits) == 12:
            return digits
        if len(digits) == 11:
            # 91 + 9 digits -> pad to 91 + 10
            return "91" + digits[2:].zfill(10)
        if len(digits) == 10:
            # "91" is first 2 of 10-digit number, add country code
            return "91" + digits
        if len(digits) > 12:
            return digits[:12]
        # 9 or fewer digits after 91
        return "91" + digits[2:].zfill(10) if len(digits) >= 2 else "91" + digits.zfill(10)
    # No 91 prefix: add India country code; take last 10 digits if longer
    if len(digits) > 10:
        digits = digits[-10:]
    return "91" + digits.zfill(10) if len(digits) <= 10 else "91" + digits


class ScoreplexClient:
    def __init__(self):
        self.base_url = settings.SCOREPLEX_BASE_URL.rstrip('/')
        self.api_key = settings.SCOREPLEX_API_KEY
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True
        )
    
    async def submit_search(self, email: str, phone: str, ip: Optional[str] = None) -> Optional[str]:
        """Submit search to Scoreplex. Payload and auth match Flask so same input gives same result (IN/India, correct carrier)."""
        try:
            normalized_phone = _normalize_phone_for_api(phone) if phone else ""
            payload = {
                "email": email or "",
                "emailAddress": email or "",
                "phone": normalized_phone,
                "mobile_number": normalized_phone,
                "mobileNumber": normalized_phone,
                "country_code": "91",
                "countryCode": "91",
            }
            if ip:
                payload["ip"] = ip
            if normalized_phone:
                print(f"📱 Phone sent to API: {normalized_phone} (len={len(normalized_phone)}, expect 12 for India)", flush=True)
            # Auth: Bearer for POST (match Flask - Scoreplex expects this for submit)
            headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
            response = await self.client.post(
                f"{self.base_url}/search",
                params={"api_key": self.api_key, "report": "false"},
                headers=headers,
                json=payload
            )
            
            print(f"📡 Submit response: {response.status_code}", flush=True)
            
            if response.status_code == 202:
                data = response.json()
                task_id = data.get("id")
                print(f"✅ Task created: {task_id[:12]}...", flush=True)
                return task_id
            else:
                print(f"❌ Submit failed: {response.status_code} - {response.text[:100]}", flush=True)
                return None
                
        except Exception as e:
            print(f"❌ Submit error: {str(e)}", flush=True)
            return None
    
    async def get_task_result(self, task_id: str) -> Optional[Dict[Any, Any]]:
        """Get task result from Scoreplex. Match Flask: API-Key header + api_key in query, report=false."""
        try:
            headers = {"API-Key": self.api_key}
            response = await self.client.get(
                f"{self.base_url}/search/task/{task_id}",
                params={"api_key": self.api_key, "report": "false"},
                headers=headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            return None
    
    # Case-insensitive completion values (match Flask so same API response is treated complete)
    _COMPLETE_VALUES = ("complete", "completed", "success", "done", "finished", "successful", "ready")
    # Alternate key names for status fields (match Flask - API may return camelCase or variants)
    _EMAIL_STATUS_KEYS = ("email_status", "emailStatus", "status_email", "email")
    _PHONE_STATUS_KEYS = ("phone_status", "phoneStatus", "status_phone", "phone")
    _DATA_LEAK_STATUS_KEYS = ("data_leak_status", "dataLeakStatus", "data_leaks_status", "leaks_status")

    def _extract_response_data(self, response: Dict[Any, Any]) -> Dict[Any, Any]:
        """Unwrap nested response (Flask-style) so status fields are at top level of returned dict."""
        if not isinstance(response, dict):
            return response
        for key in ("data", "result", "response", "body", "content", "payload", "report"):
            if key in response and isinstance(response[key], dict):
                return response[key]
        return response

    def _get_status_value(self, data: Dict[Any, Any], keys: tuple) -> Any:
        """Return first value found for any of the given keys (case-insensitive key lookup)."""
        data_lower = {str(k).lower(): v for k, v in data.items()}
        for key in keys:
            if key.lower() in data_lower:
                return data_lower[key.lower()]
        return None

    def check_statuses_complete(self, response: Dict[Any, Any]) -> tuple:
        """
        Check if email, phone, and data_leak statuses are COMPLETE (IP status is ignored).
        Unwraps response like Flask (report/data wrapper) and checks multiple key names.
        """
        # Unwrap so we look at same level as Flask after extract_response_data
        data = self._extract_response_data(response)
        if not isinstance(data, dict):
            data = {}

        statuses = {}
        statuses["email_status"] = self._get_status_value(data, self._EMAIL_STATUS_KEYS) or "PENDING"
        statuses["phone_status"] = self._get_status_value(data, self._PHONE_STATUS_KEYS) or "PENDING"
        statuses["data_leak_status"] = self._get_status_value(data, self._DATA_LEAK_STATUS_KEYS) or "PENDING"

        def _is_done(val: Any) -> bool:
            return str(val or "").strip().lower() in self._COMPLETE_VALUES

        email_done = _is_done(statuses["email_status"])
        phone_done = _is_done(statuses["phone_status"])
        data_leak_done = _is_done(statuses["data_leak_status"])

        all_complete = email_done and phone_done and data_leak_done
        return all_complete, statuses
    
    async def poll_until_ready(self, task_id: str) -> Optional[Dict[Any, Any]]:
        """
        Poll until email, phone, and data_leak are COMPLETE.
        Stops at MAX_POLL_ATTEMPTS or POLL_TIMEOUT_SECONDS (whichever first), like Flask.
        """
        print(f"🔄 Polling: {task_id[:12]}...", flush=True)
        start_time = time.time()
        response = None

        for attempt in range(1, settings.MAX_POLL_ATTEMPTS + 1):
            await asyncio.sleep(settings.POLL_INTERVAL)

            elapsed = time.time() - start_time
            if elapsed >= settings.POLL_TIMEOUT_SECONDS:
                print(f"  ⚠️ Timeout after {elapsed:.0f}s (POLL_TIMEOUT_SECONDS={settings.POLL_TIMEOUT_SECONDS})", flush=True)
                break

            response = await self.get_task_result(task_id)
            if not response:
                if attempt % 10 == 1:
                    print(f"  ⚠️ Attempt {attempt}: No response from Scoreplex", flush=True)
                continue

            all_complete, statuses = self.check_statuses_complete(response)

            if attempt % 5 == 1 or all_complete:
                print(f"  Attempt {attempt}/{settings.MAX_POLL_ATTEMPTS}: {statuses}", flush=True)

            if all_complete:
                print(f"  ✅ Complete after {attempt} attempts", flush=True)
                return response

        if not response and attempt >= settings.MAX_POLL_ATTEMPTS:
            print(f"  ⚠️ Timeout after {settings.MAX_POLL_ATTEMPTS} attempts", flush=True)
        if response:
            _, final_statuses = self.check_statuses_complete(response)
            print(f"  Final: {final_statuses}", flush=True)
        return response
    
    async def process_row(self, email: str, phone: str, ip: Optional[str] = None) -> Dict[Any, Any]:
        """
        Process one row: submit → poll → return response
        """
        print(f"\n🔹 Processing: {email} / {phone}", flush=True)
        
        task_id = await self.submit_search(email, phone, ip)
        
        if not task_id:
            return {
                "status": "FAILED",
                "error": "Failed to submit",
                "email": email,
                "phone": phone
            }
        
        result = await self.poll_until_ready(task_id)
        
        if not result:
            return {
                "status": "FAILED",
                "error": "No result from polling",
                "task_id": task_id,
                "email": email,
                "phone": phone
            }
        
        # Only SUCCESS when all 3 statuses (email, phone, data_leak) are complete
        all_complete, statuses = self.check_statuses_complete(result)
        if not all_complete:
            print(f"  ⚠️ Row incomplete (timeout or pending): {email} - {statuses}", flush=True)
            return {
                "status": "INCOMPLETE",
                "error": "Timeout or not all checks complete (email, phone, data_leak)",
                "task_id": task_id,
                "email": email,
                "phone": phone,
                "raw_response": result
            }
        
        print(f"  ✅ Row complete: {email}", flush=True)
        return {
            "status": "SUCCESS",
            "task_id": task_id,
            "email": email,
            "phone": phone,
            "raw_response": result
        }
    
    async def close(self):
        await self.client.aclose()
