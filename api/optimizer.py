import re
import nltk
from nltk.corpus import stopwords

# Ensure resources
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

class PromptOptimizer:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))

        # Keep important words (very important for meaning)
        self.keep_words = {
            "not", "no", "how", "what", "why", "who", "when", "where",
            "explain", "define", "compare", "list", "give", "examples"
        }

        self.stop_words = self.stop_words - self.keep_words

        # Filler phrases (expanded)
        self.fillers = [
            "please", "could you", "would you mind", "kindly",
            "i would like you to", "can you", "tell me", "help me understand"
        ]

    def optimize(self, prompt: str) -> str:
        if not isinstance(prompt, str):
            return ""

        original = prompt

        # 1️⃣ Lowercase
        optimized = prompt.lower()

        # 2️⃣ Remove filler phrases
        for filler in self.fillers:
            optimized = optimized.replace(filler, "")

        # 3️⃣ Remove punctuation (keep meaning)
        optimized = re.sub(r'[^\w\s]', '', optimized)

        # 4️⃣ Tokenize
        words = optimized.split()

        # 5️⃣ Remove stopwords (but keep important ones)
        compressed_words = [w for w in words if w not in self.stop_words]

        # 6️⃣ Remove duplicates (smart compression)
        seen = set()
        unique_words = []
        for w in compressed_words:
            if w not in seen:
                unique_words.append(w)
                seen.add(w)

        # 7️⃣ Reconstruct
        optimized = " ".join(unique_words)

        # 8️⃣ Fallback (avoid empty output)
        if len(optimized.strip()) == 0:
            return original

        return optimized