
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PySide6.QtWidgets import QApplication
from src.ui.main_window import ReabKidsGame
from src.utils.image_utils import create_all_assets

def main():
    """Função principal."""
    image_data_dir = os.path.join('../data', 'images')
    audio_data_dir = os.path.join('../data', 'audios')

    if not os.path.exists(image_data_dir):
        os.makedirs(image_data_dir)
        print(
            f"AVISO: Pasta '{image_data_dir}' não encontrada. Será criada. Imagens não presentes serão buscadas online.")

    if not os.path.exists(audio_data_dir):
        os.makedirs(audio_data_dir)
        print(
            f"AVISO: Pasta '{audio_data_dir}' não encontrada. Será criada.")

    create_all_assets(image_data_dir)

    app = QApplication([])
    game = ReabKidsGame()
    game.show()
    app.exec()

if __name__ == '__main__':
    main()
