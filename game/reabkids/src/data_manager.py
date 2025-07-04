
import json
import os
from PySide6.QtWidgets import QMessageBox

class DataManager:
    def __init__(self, questions_file=os.path.join('./data', 'perguntas.json')):
        self.questions_file = questions_file
        self.all_questions = []

    def load_questions(self):
        if os.path.exists(self.questions_file):
            try:
                with open(self.questions_file, 'r', encoding='utf-8') as f:
                    self.all_questions = json.load(f)
            except json.JSONDecodeError as e:
                QMessageBox.critical(None, "Erro de Leitura de Perguntas",
                                     f"Não foi possível decodificar o JSON do arquivo '{self.questions_file}'. "
                                     f"Por favor, verifique a sintaxe do arquivo. Erro: {e}")
                self.all_questions = []
            except FileNotFoundError:
                QMessageBox.warning(None, "Arquivo de Perguntas Não Encontrado",
                                    f"O arquivo de perguntas '{self.questions_file}' não foi encontrado. "
                                    f"O jogo não terá perguntas.")
                self.all_questions = []
            except Exception as e:
                QMessageBox.critical(None, "Erro ao Carregar Perguntas",
                                     f"Ocorreu um erro inesperado ao carregar perguntas: {e}")
                self.all_questions = []
        else:
            QMessageBox.information(None, "Arquivo de Perguntas Ausente",
                                    f"O arquivo de perguntas '{self.questions_file}' não foi encontrado. "
                                    f"Certifique-se de que o arquivo 'perguntas.json' está na pasta 'data'. "
                                    f"O jogo não terá perguntas.")
            self.all_questions = []
        return self.all_questions
