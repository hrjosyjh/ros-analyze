"""Internationalization support for ROS2 log analyzer tools.

Supported languages: ko (Korean), en (English)
Default: ko
"""

_lang = 'ko'


def set_lang(lang: str):
    """Set the current language. Supported: 'ko', 'en'."""
    global _lang
    if lang not in ('ko', 'en'):
        raise ValueError(f"Unsupported language: {lang}. Use 'ko' or 'en'.")
    _lang = lang


def get_lang() -> str:
    """Get the current language."""
    return _lang


def t(ko: str, en: str) -> str:
    """Return the message in the current language.

    Usage:
        t("한국어 메시지", "English message")
    """
    return ko if _lang == 'ko' else en
