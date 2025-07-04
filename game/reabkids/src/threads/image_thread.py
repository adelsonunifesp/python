
import os
import requests
from PySide6.QtCore import QThread, Signal

PEXELS_API_KEY = "cn4333QuLvQTNxbBrQczTGXvBdXROo9ZUhiFU7M8Q7Bathf4Bq2zTTsC"  # Sua API Key Pexels (use uma válida)

class ImageDownloadThread(QThread):
    finished = Signal(str, str)  # Emite o caminho da imagem e o nome do arquivo original
    error = Signal(str, str)  # Emite a mensagem de erro e o nome do arquivo original
    progress = Signal(str)  # Sinal para indicar progresso ou status

    def __init__(self, search_term, target_filename, image_dir):
        super().__init__()
        self.search_term = search_term
        self.target_filename = target_filename
        self.image_dir = image_dir

    def run(self):
        self.progress.emit(f"Buscando '{self.search_term}'...")
        pexels_search_url = f"https://api.pexels.com/v1/search?query={self.search_term}&per_page=1&orientation=square"
        headers = {
            "Authorization": PEXELS_API_KEY
        }

        try:
            response = requests.get(pexels_search_url, headers=headers)
            response.raise_for_status()

            data = response.json()

            if 'photos' in data and len(data['photos']) > 0:
                image_url = data['photos'][0]['src']['medium']
                self.progress.emit(f"Baixando '{self.target_filename}'...")
                image_response = requests.get(image_url, stream=True)
                image_response.raise_for_status()

                save_path = os.path.join(self.image_dir, self.target_filename)

                with open(save_path, 'wb') as out_file:
                    for chunk in image_response.iter_content(chunk_size=8192):
                        out_file.write(chunk)
                self.finished.emit(save_path, self.target_filename)
            else:
                # Se nenhuma imagem for encontrada, não emita um erro, apenas termine silenciosamente.
                # A UI tratará a ausência do arquivo.
                self.finished.emit(None, self.target_filename)  # Emite None para indicar falha

        except requests.exceptions.RequestException as e:
            self.error.emit(f"Erro de conexão ou requisição ao Pexels para '{self.search_term}': {e}",
                            self.target_filename)
        except Exception as e:
            self.error.emit(f"Erro inesperado ao buscar imagem para '{self.search_term}': {e}", self.target_filename)
