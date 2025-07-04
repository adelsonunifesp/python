
from PySide6.QtCore import QThread, Signal
from gtts import gTTS

class AudioGenerationThread(QThread):
    finished = Signal(str)  # Emite o nome do arquivo gerado
    error = Signal(str)

    def __init__(self, text, filename, lang, tld):
        super().__init__()
        self.text = text
        self.filename = filename
        self.lang = lang
        self.tld = tld

    def run(self):
        try:
            tts = gTTS(text=self.text, lang=self.lang, tld=self.tld, slow=False)
            tts.save(self.filename)
            self.finished.emit(self.filename)
        except Exception as e:
            self.error.emit(f"ERRO ao gerar áudio: {e}")
