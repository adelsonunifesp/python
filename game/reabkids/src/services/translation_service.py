
from googletrans import Translator

class TranslationService:
    def __init__(self):
        self.translator = Translator()

    def translate(self, text, dest_lang, src_lang='pt'):
        try:
            translated_text_obj = self.translator.translate(text, dest=dest_lang, src=src_lang)
            return translated_text_obj.text
        except Exception as e:
            print(f"ERRO na tradução: {e}. Usando texto original.")
            return text
