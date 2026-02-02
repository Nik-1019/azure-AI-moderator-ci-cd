import os
import json
import requests
import re

def check_message(message_content: str) -> dict:
    """Check message content for toxic content using Gemini API."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"is_allowed": True, "category": "clean", "confidence": 0.0, "reason": "API key not configured"}
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
    
    prompt = f"""Analyze this message for toxic content and respond with ONLY a JSON object:
{{
  "category": "clean|toxic|spam|harassment",
  "confidence": 0.0-1.0,
  "reason": "brief explanation"
}}

Message: {message_content}"""
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1}
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        text = result["candidates"][0]["content"]["parts"][0]["text"]
        
        # Strip markdown code blocks
        text = re.sub(r'```(?:json)?\n?', '', text).strip()
        
        data = json.loads(text)
        is_allowed = data["category"] == "clean"
        
        return {
            "is_allowed": is_allowed,
            "category": data["category"],
            "confidence": float(data["confidence"]),
            "reason": data["reason"]
        }
    
    except Exception:
        # Fail-open pattern
        return {"is_allowed": True, "category": "clean", "confidence": 0.0, "reason": "API error - allowing message"}