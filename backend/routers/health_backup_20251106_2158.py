from fastapi import APIRouter

from backend.services import env_utils

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/debug/llm")
async def debug_llm():
    """Debug endpoint for LLM smoke testing (dev only)."""
    if env_utils.get_environment().lower() == "prod":
        return {"error": "LLM debug disabled in production"}
    try:
        from backend.services import llm_utils
        # Use a safe test input
        test_text = "Apple announces new iPhone with improved battery life."
        result = await llm_utils.async_classify_headline_and_summary(test_text)
        return {
            "input": test_text,
            "headline": result.get("headline_final"),
            "summary": result.get("summary_60"),
            "meta": result.get("llm_meta"),
        }
    except Exception as e:
        return {"error": str(e)}
