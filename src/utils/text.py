import re

def exact_word_match(text: str, word: str) -> bool:
    if not text:
        return False
    # точное слово: границы не-символов слова
    pattern = re.compile(rf"(?<![\w]){re.escape(word)}(?![\w])", re.IGNORECASE)
    return bool(pattern.search(text))
