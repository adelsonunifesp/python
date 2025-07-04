import os
import random
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
    QWidget, QLabel, QPushButton, QGridLayout,
    QCheckBox, QMessageBox
)
from PySide6.QtGui import QPixmap, QFont, QIcon, QGuiApplication
from PySide6.QtCore import Qt, QTimer, QUrl, QSize, QPropertyAnimation, QEasingCurve, QPoint, QEvent
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput
from PIL import Image, ImageQt, ImageDraw, ImageFont

from src.threads.audio_thread import AudioGenerationThread
from src.threads.image_thread import ImageDownloadThread
from src.services.translation_service import TranslationService
from src.data_manager import DataManager

class ReabKidsGame(QMainWindow):
    VOICE_OPTIONS = {
        "Português BR": {'lang': 'pt', 'tld': 'com.br'},
        "English US": {'lang': 'en', 'tld': 'us'},
    }

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ReabKids - Jogo Educativo")
        screen_geometry = QGuiApplication.primaryScreen().availableGeometry()
        self.window_width = 750
        self.window_height = 990
        self.setGeometry(
            int((screen_geometry.width() - self.window_width) / 2),
            int((screen_geometry.height() - self.window_height) / 2),
            self.window_width,
            self.window_height
        )
        self.setFixedSize(self.window_width, self.window_height)
        self.setWindowFlags(Qt.FramelessWindowHint)
        self.setStyleSheet("QMainWindow { background-color: #F5F5DC; }")

        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(base_dir, '..', '..'))
        self.audio_dir = os.path.join(project_root, 'data', 'audios')
        if not os.path.exists(self.audio_dir):
            os.makedirs(self.audio_dir)

        self.image_dir = os.path.join(project_root, 'data', 'images')
        if not os.path.exists(self.image_dir):
            os.makedirs(self.image_dir)

        self.data_manager = DataManager()
        self.all_questions = self.data_manager.load_questions()

        self.media_player = QMediaPlayer()
        self.audio_output = QAudioOutput()
        self.media_player.setAudioOutput(self.audio_output)

        self.current_question = None
        self.option_buttons = []
        self.pixmap_cache = {}
        self.image_download_threads = []
        self.unanswered_questions = []
        self._populate_unanswered_questions()

        self.current_voice_config = self.VOICE_OPTIONS["Português BR"]
        self.translation_service = TranslationService()
        self.sound_enabled = True
        self.menu_expanded = False
        self.menu_width_expanded = 180
        self.menu_width_collapsed = 70
        self.action_button_text_size = QSize(150, 40)
        self.action_button_icon_size_collapsed = QSize(40, 40)
        self.internal_icon_size = QSize(40, 40)

        self.menu_action_button_style = """
            QPushButton {
                background-color: #A0D9B9;
                color: #555555;
                font-size: 16px;
                font-weight: bold;
                border-radius: 10px;
                padding: 10px 1px;
                border: 1px solid #7EC89F;
            }
            QPushButton:hover {
                background-color: #7EC89F;
            }
            QPushButton:pressed {
                background-color: #5AA17A;
            }
        """
        self.menu_icon_button_style = """
            QPushButton {
                background-color: #A0D9B9;
                border-radius: 5px;
                padding: 2px;
                border: 1px solid #7EC89F;
                min-width: 38px;
                max-width: 38px;
                min-height: 38px;
                max-height: 38px;
            }
            QPushButton:hover {
                background-color: #7EC89F;
            }
            QPushButton:pressed {
                background-color: #5AA17A;
            }
             /* Para imagens PNG com transparência, o fundo do botão aparecerá */
            QPushButton::icon {
                background-color: transparent; /* Garante que o ícone em si não tenha um fundo opaco */
            }
        """

        self.dragging = False
        self.offset = QPoint()

        self.setup_ui()
        self.load_next_question()

    def eventFilter(self, watched, event):
        if watched == self.title_bar:
            if event.type() == QEvent.MouseButtonPress:
                if event.button() == Qt.LeftButton:
                    self.dragging = True
                    self.offset = event.globalPosition().toPoint() - self.pos()
                    return True
            elif event.type() == QEvent.MouseMove:
                if self.dragging and event.buttons() == Qt.LeftButton:
                    self.move(event.globalPosition().toPoint() - self.offset)
                    return True
            elif event.type() == QEvent.MouseButtonRelease:
                if event.button() == Qt.LeftButton:
                    self.dragging = False
                    return True
        return super().eventFilter(watched, event)

    def _populate_unanswered_questions(self):
        if not self.all_questions:
            self.unanswered_questions = []
            return

        self.unanswered_questions = self.all_questions[:]
        random.shuffle(self.unanswered_questions)

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_v_layout = QVBoxLayout(central_widget)
        main_v_layout.setContentsMargins(0, 0, 0, 0)
        main_v_layout.setSpacing(0)

        self.title_bar = QWidget()
        self.title_bar.setFixedHeight(50)
        self.title_bar.setStyleSheet(
            "background-color: #B0E0E6; border-top-left-radius: 0px; border-top-right-radius: 0px;")
        self.title_bar.installEventFilter(self)

        title_bar_layout = QHBoxLayout(self.title_bar)
        title_bar_layout.setContentsMargins(10, 0, 10, 0)
        title_bar_layout.setSpacing(0)

        self.game_title_label_bar = QLabel(
            "<h2 align='left' style='color: #4682B4; font-size: 30px; text-shadow: 1px 1px 1px #87CEEB;'>ReabKids</h2>"
        )
        self.game_title_label_bar.setAlignment(Qt.AlignCenter)
        title_bar_layout.addWidget(self.game_title_label_bar)

        self.custom_close_btn = QPushButton("X")
        self.custom_close_btn.setFixedSize(30, 30)
        self.custom_close_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF6B6B;
                color: white;
                font-weight: bold;
                border-radius: 15px;
            }
            QPushButton:hover {
                background-color: #FF8E8E;
            }
            QPushButton:pressed {
                background-color: #FF4D4D;
            }
        """)
        self.custom_close_btn.clicked.connect(QApplication.instance().quit)
        title_bar_layout.addWidget(self.custom_close_btn, alignment=Qt.AlignRight | Qt.AlignVCenter)

        main_v_layout.addWidget(self.title_bar)

        self.content_h_layout = QHBoxLayout()
        self.content_h_layout.setContentsMargins(10, 0, 10, 10)
        self.content_h_layout.setSpacing(20)

        self.menu_frame = QWidget()
        self.menu_frame.setStyleSheet(
            "background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #D8F2E2, stop:1 #A0D9B9); border-radius: 15px;")
        self.menu_frame.setFixedWidth(self.menu_width_collapsed)
        self.menu_layout = QVBoxLayout(self.menu_frame)
        self.menu_layout.setContentsMargins(15, 20, 15, 20)
        self.menu_layout.setSpacing(15)
        self.menu_layout.setAlignment(Qt.AlignTop)

        self.toggle_menu_btn = QPushButton("☰")
        self.toggle_menu_btn.setFont(QFont("Arial", 20, QFont.Bold))
        self.toggle_menu_btn.setToolTip("Expandir Menu")
        self.toggle_menu_btn.setStyleSheet("""
            QPushButton {
                background-color: #A0D9B9;
                color: #555555;
                border-radius: 10px;
                padding: 1px;
                border: 1px solid #7EC89F;
            }
            QPushButton:hover {
                background-color: #7EC89F;
            }
        """)
        self.toggle_menu_btn.setFixedSize(40, 40)
        self.toggle_menu_btn.clicked.connect(self.toggle_menu_visibility)
        self.menu_layout.addWidget(self.toggle_menu_btn, alignment=Qt.AlignRight)
        self.menu_layout.addSpacing(10)

        self.menu_title_label = QLabel("Menu")
        self.menu_title_label.setFont(QFont("Arial", 18, QFont.Bold))
        self.menu_title_label.setStyleSheet("color: #555555;")
        self.menu_title_label.setAlignment(Qt.AlignCenter)
        self.menu_layout.addWidget(self.menu_title_label)
        self.menu_layout.addSpacing(20)
        self.menu_title_label.hide()

        self.btn_start_game = QPushButton("Iniciar Jogo")
        self.btn_start_game.clicked.connect(self.load_next_question)
        self.menu_layout.addWidget(self.btn_start_game)
        self.btn_start_game.setProperty("original_text", "Iniciar Jogo")
        self.btn_start_game.setProperty("icon_object", QIcon(
            self.load_image_pyside(os.path.join(self.image_dir, "play_icon.png"), size=(30, 30))))
        self.btn_start_game.setToolTip("Iniciar Jogo")

        self.btn_settings = QPushButton("Configurações")
        self.btn_settings.clicked.connect(self.toggle_settings_visibility)
        self.menu_layout.addWidget(self.btn_settings)
        self.btn_settings.setProperty("original_text", "Configurações")
        self.btn_settings.setProperty("icon_object", QIcon(
            self.load_image_pyside(os.path.join(self.image_dir, "settings_icon.png"), size=(30, 30))))
        self.btn_settings.setToolTip("Configurações")

        self.menu_layout.addStretch()

        menu_action_buttons = [self.btn_start_game, self.btn_settings]
        for btn in menu_action_buttons:
            btn.setText("")
            icon_obj = btn.property("icon_object")
            if icon_obj:
                btn.setIcon(icon_obj)
                btn.setIconSize(self.internal_icon_size)
            else:
                btn.setText(btn.property("original_text")[0])
                btn.setIcon(QIcon())
            btn.setFixedSize(self.action_button_icon_size_collapsed)
            btn.setStyleSheet(self.menu_icon_button_style)

        self.content_h_layout.addWidget(self.menu_frame)

        self.game_content_frame = QWidget()
        self.game_content_frame.setStyleSheet(
            "background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #FFDAB9, stop:1 #FFE4E1); border-radius: 15px;")
        game_content_layout = QVBoxLayout(self.game_content_frame)
        game_content_layout.setContentsMargins(20, 20, 20, 20)
        game_content_layout.setSpacing(20)

        self.pergunta_label = QLabel("Bem-vindo ao ReabKids!")
        self.pergunta_label.setFont(QFont("Arial", 24, QFont.Bold))
        self.pergunta_label.setStyleSheet("color: #333333;")
        self.pergunta_label.setAlignment(Qt.AlignCenter)
        self.pergunta_label.setWordWrap(True)
        game_content_layout.addWidget(self.pergunta_label)

        self.status_label = QLabel("")
        self.status_label.setFont(QFont("Arial", 14))
        self.status_label.setStyleSheet("color: #666666; font-style: italic;")
        self.status_label.setAlignment(Qt.AlignCenter)
        game_content_layout.addWidget(self.status_label)
        self.status_label.hide()

        self.opcoes_grid_layout = QGridLayout()
        self.opcoes_grid_layout.setSpacing(30)
        self.opcoes_grid_layout.setContentsMargins(30, 30, 30, 30)
        self.opcoes_grid_layout.setAlignment(Qt.AlignCenter)
        game_content_layout.addLayout(self.opcoes_grid_layout)
        game_content_layout.addStretch()

        self.settings_widget = QWidget()
        self.settings_widget.setStyleSheet(
            "background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #ADD8E6, stop:1 #B0E0E6); border-radius: 10px; padding: 5px;")
        self.settings_h_layout = QHBoxLayout(self.settings_widget)
        self.settings_h_layout.setAlignment(Qt.AlignRight | Qt.AlignBottom)
        self.settings_h_layout.setContentsMargins(5, 5, 5, 5)
        self.settings_h_layout.setSpacing(10)

        self.sound_checkbox = QCheckBox("Ativar Som")
        self.sound_checkbox.setFont(QFont("Arial", 12))
        self.sound_checkbox.setChecked(True)
        self.sound_checkbox.setStyleSheet("color: #555555;")
        self.sound_checkbox.stateChanged.connect(self.toggle_sound)
        self.settings_h_layout.addWidget(self.sound_checkbox)

        flag_button_size = 60
        icon_size = 50

        self.brazil_flag_btn = QPushButton()
        self.brazil_flag_btn.setFixedSize(flag_button_size, flag_button_size)
        self.brazil_flag_btn.setIconSize(QSize(icon_size, icon_size))
        brazil_flag_pixmap = self.load_image_pyside(os.path.join(self.image_dir, "brazil_flag.png"),
                                                    size=(icon_size, icon_size))
        self.brazil_flag_btn.setIcon(QIcon(brazil_flag_pixmap))
        self.brazil_flag_btn.setToolTip("Português do Brasil")
        self.brazil_flag_btn.setCheckable(True)
        self.brazil_flag_btn.setChecked(True)
        self.brazil_flag_btn.clicked.connect(lambda: self.set_language("Português BR"))
        self.settings_h_layout.addWidget(self.brazil_flag_btn)

        self.us_flag_btn = QPushButton()
        self.us_flag_btn.setFixedSize(flag_button_size, flag_button_size)
        self.us_flag_btn.setIconSize(QSize(icon_size, icon_size))
        us_flag_pixmap = self.load_image_pyside(os.path.join(self.image_dir, "us_flag.png"),
                                                size=(icon_size, icon_size))
        self.us_flag_btn.setIcon(QIcon(us_flag_pixmap))
        self.us_flag_btn.setToolTip("English (US)")
        self.us_flag_btn.setCheckable(True)
        self.us_flag_btn.setChecked(False)
        self.us_flag_btn.clicked.connect(lambda: self.set_language("English US"))
        self.settings_h_layout.addWidget(self.us_flag_btn)

        flag_button_style_round = f"""
            QPushButton {{
                border-radius: {flag_button_size // 2}px;
                border: 2px solid #90CAF9;
                background-color: #E0F7FA;
                margin: 5px;
            }}
            QPushButton:hover {{
                border: 2px solid #64B5F6;
            }}
            QPushButton:checked {{
                border: 4px solid #2196F3;
                background-color: #BBDEFB;
            }}
        """
        self.brazil_flag_btn.setStyleSheet(flag_button_style_round)
        self.us_flag_btn.setStyleSheet(flag_button_style_round)

        game_content_layout.addWidget(self.settings_widget)
        self.settings_widget.hide()
        self.settings_visible = False

        self.content_h_layout.addWidget(self.game_content_frame)
        main_v_layout.addLayout(self.content_h_layout)

    def toggle_settings_visibility(self):
        if self.settings_visible:
            self.settings_widget.hide()
            self.settings_visible = False
        else:
            self.settings_widget.show()
            self.settings_visible = True

    def toggle_menu_visibility(self):
        start_width = self.menu_frame.width()
        action_button_text_size = self.action_button_text_size
        action_button_icon_size_collapsed = self.action_button_icon_size_collapsed
        internal_icon_size = self.internal_icon_size
        menu_action_buttons = [self.btn_start_game, self.btn_settings]

        if self.menu_expanded:
            end_width = self.menu_width_collapsed
            self.menu_title_label.hide()
            self.toggle_menu_btn.setToolTip("Expandir Menu")
            for btn in menu_action_buttons:
                btn.setText("")
                icon_obj = btn.property("icon_object")
                if icon_obj:
                    btn.setIcon(icon_obj)
                    btn.setIconSize(internal_icon_size)
                else:
                    btn.setText(btn.property("original_text")[0])
                    btn.setIcon(QIcon())
                btn.setFixedSize(action_button_icon_size_collapsed)
                btn.setStyleSheet(self.menu_icon_button_style)
        else:
            end_width = self.menu_width_expanded
            self.menu_title_label.show()
            self.toggle_menu_btn.setToolTip("Recolher Menu")
            for btn in menu_action_buttons:
                btn.setText(btn.property("original_text"))
                btn.setIcon(QIcon())
                btn.setFixedSize(action_button_text_size)
                btn.setStyleSheet(self.menu_action_button_style)

        self.animation_menu = QPropertyAnimation(self.menu_frame, b"minimumWidth", self)
        self.animation_menu.setDuration(300)
        self.animation_menu.setStartValue(start_width)
        self.animation_menu.setEndValue(end_width)
        self.animation_menu.setEasingCurve(QEasingCurve.InOutQuad)
        self.animation_menu.start()
        self.menu_expanded = not self.menu_expanded

    def set_language(self, language_name):
        if language_name == "Português BR":
            self.current_voice_config = self.VOICE_OPTIONS["Português BR"]
            self.brazil_flag_btn.setChecked(True)
            self.us_flag_btn.setChecked(False)
            self.animate_button(self.brazil_flag_btn)
        elif language_name == "English US":
            self.current_voice_config = self.VOICE_OPTIONS["English US"]
            self.us_flag_btn.setChecked(True)
            self.brazil_flag_btn.setChecked(False)
            self.animate_button(self.us_flag_btn)

        if self.current_question:
            self._display_current_question_options()

    def animate_button(self, button):
        if hasattr(button, '_current_animation_grow') and button._current_animation_grow.state() == QPropertyAnimation.Running:
            button._current_animation_grow.stop()
        if hasattr(button, '_current_animation_shrink') and button._current_animation_shrink.state() == QPropertyAnimation.Running:
            button._current_animation_shrink.stop()

        original_size = button.size()
        target_size_grow = QSize(int(original_size.width() * 1.05), int(original_size.height() * 1.05))

        animation_grow = QPropertyAnimation(button, b"size", self)
        animation_grow.setDuration(100)
        animation_grow.setStartValue(original_size)
        animation_grow.setEndValue(target_size_grow)
        animation_grow.setEasingCurve(QEasingCurve.OutQuad)

        animation_shrink = QPropertyAnimation(button, b"size", self)
        animation_shrink.setDuration(100)
        animation_shrink.setStartValue(target_size_grow)
        animation_shrink.setEndValue(original_size)
        animation_shrink.setEasingCurve(QEasingCurve.InQuad)

        animation_grow.finished.connect(animation_shrink.start)

        button._current_animation_grow = animation_grow
        button._current_animation_shrink = animation_shrink

        animation_grow.start()

    def toggle_sound(self, state):
        self.sound_enabled = bool(state)

    def _play_sound(self, filename):
        if not self.sound_enabled:
            return

        if not os.path.exists(filename):
            print(f"ERRO: Arquivo de áudio não encontrado para reprodução: {filename}")
            return

        try:
            self.media_player.setSource(QUrl.fromLocalFile(filename))
            self.media_player.play()
        except Exception as e:
            print(f"ERRO ao reproduzir áudio: {e}")

    def falar_e_reproduzir(self, texto_original):
        if not self.sound_enabled:
            return

        target_lang_for_speech = self.current_voice_config['lang']
        texto_para_falar = self.translation_service.translate(texto_original, dest_lang=target_lang_for_speech)

        safe_filename = texto_para_falar.replace(' ', '_').replace('?', '').replace('!', '').replace('.', '').replace(
            ',', '').lower()
        voice_id = f"{self.current_voice_config['lang']}_{self.current_voice_config['tld']}"
        filepath = os.path.join(self.audio_dir, f"{safe_filename}_{voice_id}.mp3")

        if os.path.exists(filepath):
            self._play_sound(filepath)
        else:
            self.audio_generation_thread = AudioGenerationThread(
                texto_para_falar,
                filepath,
                lang=self.current_voice_config['lang'],
                tld=self.current_voice_config['tld']
            )
            self.audio_generation_thread.finished.connect(self._play_sound)
            self.audio_generation_thread.error.connect(lambda msg: print(msg))
            self.audio_generation_thread.start()

    def _update_button_image(self, image_path, original_filename):
        self.status_label.hide()
        if self.current_question:
            self._display_current_question_options()

    def load_image_pyside(self, image_file_path, size=(200, 200)):
        image_filename = os.path.basename(image_file_path)

        if image_filename in self.pixmap_cache:
            return self.pixmap_cache[image_filename]

        try:
            if os.path.exists(image_file_path):
                pil_image = Image.open(image_file_path)
                if pil_image.mode == 'LA' or pil_image.mode == 'RGBA':
                    background = Image.new('RGB', pil_image.size, (255, 255, 255))
                    background.paste(pil_image, (0, 0), pil_image)
                    pil_image = background

                pil_image.thumbnail(size, Image.Resampling.LANCZOS)

                final_pil_image = Image.new('RGB', size, color='white')
                x_offset = (size[0] - pil_image.width) // 2
                y_offset = (size[1] - pil_image.height) // 2
                final_pil_image.paste(pil_image, (x_offset, y_offset))

                q_image = ImageQt.ImageQt(final_pil_image)
                pixmap = QPixmap.fromImage(q_image)
                self.pixmap_cache[image_filename] = pixmap
                return pixmap
            else:
                from src.threads.image_thread import PEXELS_API_KEY
                if PEXELS_API_KEY == "":
                    print("AVISO: PEXELS_API_KEY não configurada. Imagens não serão baixadas.")
                    placeholder = Image.new('RGB', size, color='red')
                    q_image = ImageQt.ImageQt(placeholder)
                    pixmap = QPixmap.fromImage(q_image)
                    self.pixmap_cache[image_filename] = pixmap
                    return pixmap

                print(f"Imagem '{image_filename}' não encontrada localmente. Tentando buscar online no Pexels.")
                self.status_label.setText(f"Procurando imagem para: {os.path.splitext(image_filename)[0]}...")
                self.status_label.show()
                QApplication.processEvents()

                search_term = os.path.splitext(image_filename)[0]

                # Verifica se um download para esta imagem já está em andamento
                if any(thread.image_filename == image_filename for thread in self.image_download_threads):
                    print(f"Download para {image_filename} já está em andamento.")
                else:
                    search_term_for_pexels = self.translation_service.translate(search_term, dest_lang='en')
                    download_thread = ImageDownloadThread(search_term_for_pexels, image_filename, self.image_dir)
                    self.image_download_threads.append(download_thread)

                    download_thread.finished.connect(self._update_button_image)
                    download_thread.error.connect(lambda msg, filename: self.status_label.hide())

                placeholder = Image.new('RGB', size, color='#D3D3D3')
                draw = ImageDraw.Draw(placeholder)
                text = "Carregando..."
                font_size = 20
                try:
                    font = ImageFont.truetype("arial.ttf", font_size)
                except IOError:
                    font = ImageFont.load_default()

                draw.text((size[0] / 2, size[1] / 2), text, anchor="mm", font=font, fill=(0, 0, 0))

                q_image = ImageQt.ImageQt(placeholder)
                pixmap = QPixmap.fromImage(q_image)
                self.pixmap_cache[image_filename] = pixmap
                return pixmap
        except Exception as e:
            print(f"Erro ao carregar ou processar imagem {image_file_path}: {e}")
            QMessageBox.warning(self, "Erro de Imagem", f"Erro inesperado ao processar imagem: {e}")
            placeholder = Image.new('RGB', size, color='red')
            q_image = ImageQt.ImageQt(placeholder)
            pixmap = QPixmap.fromImage(q_image)
            self.pixmap_cache[image_filename] = pixmap
            return pixmap

    def load_next_question(self):
        for thread in self.image_download_threads:
            if thread.isRunning():
                thread.quit()
                thread.wait(1000)
                if thread.isRunning():
                    print(
                        f"Aviso: Thread de download {thread.search_term} ainda em execução ao carregar nova pergunta.")
        self.image_download_threads.clear()

        if not self.unanswered_questions:
            if not self.all_questions:
                QMessageBox.critical(self, "Sem Perguntas",
                                     "Não há perguntas carregadas para o jogo. "
                                     "Por favor, verifique o arquivo '../data/perguntas.json'.")
                self.pergunta_label.setText("Nenhuma pergunta disponível. Verifique o arquivo de perguntas.")
                for button in self.option_buttons:
                    self.opcoes_grid_layout.removeWidget(button)
                    button.deleteLater()
                self.option_buttons.clear()
                return

            self._populate_unanswered_questions()

        self.current_question = self.unanswered_questions.pop(0)

        self._display_current_question_options()
        self.status_label.hide()

    def _display_current_question_options(self):
        for button in self.option_buttons:
            self.opcoes_grid_layout.removeWidget(button)
            button.deleteLater()
        self.option_buttons.clear()
        self.pixmap_cache.clear()

        current_display_lang = self.current_voice_config['lang']
        text_to_display = self.translation_service.translate(self.current_question['texto'], dest_lang=current_display_lang)

        self.pergunta_label.setText(text_to_display)
        self.pergunta_label.setStyleSheet("color: #333333;")

        self.falar_e_reproduzir(self.current_question['texto'])

        opcoes_embaralhadas = self.current_question['opcoes'].copy()
        random.shuffle(opcoes_embaralhadas)

        cols = 2
        for i, opcao_img_filename in enumerate(opcoes_embaralhadas):
            row = i // cols
            col = i % cols

            image_file_path = os.path.join(self.image_dir, opcao_img_filename)
            pixmap = self.load_image_pyside(image_file_path, size=(200, 200))

            btn = QPushButton()
            btn.setIcon(pixmap)
            btn.setIconSize(pixmap.size())
            btn.setFixedSize(pixmap.size())
            btn.setStyleSheet("""
                QPushButton {
                    border: 3px solid #e0e0e0;
                    border-radius: 10px;
                    background-color: white;
                    margin: 15px;
                }
                QPushButton:hover {
                    background-color: #a0a0a0;
                }
                QPushButton:pressed {
                    background-color: #e0e0e0;
                }
            """)
            btn.clicked.connect(
                lambda checked, filename=opcao_img_filename, button=btn: self.handle_option_click(filename, button))

            self.opcoes_grid_layout.addWidget(btn, row, col, Qt.AlignCenter)
            self.option_buttons.append(btn)

        for c in range(cols):
            self.opcoes_grid_layout.setColumnStretch(c, 1)

    def handle_option_click(self, selected_image_filename, button):
        self.animate_button(button)
        QTimer.singleShot(100, lambda: self.check_answer(selected_image_filename))

    def check_answer(self, selected_image_filename):
        if selected_image_filename == self.current_question['resposta']:
            display_text = self.translation_service.translate("Acertou! Muito bem!", dest_lang=self.current_voice_config['lang'])
            self.pergunta_label.setText(display_text)
            self.pergunta_label.setStyleSheet("color: green; font-weight: bold;")
            self.falar_e_reproduzir("Acertou! Muito bem!")
            QTimer.singleShot(2500, self.load_next_question)
        else:
            display_text = self.translation_service.translate("Ops! Tente de novo.", dest_lang=self.current_voice_config['lang'])
            self.pergunta_label.setText(display_text)
            self.pergunta_label.setStyleSheet("color: red; font-weight: bold;")
            self.falar_e_reproduzir("Ops! Tente de novo.")
            QTimer.singleShot(2500, self._show_and_speak_current_question)

    def _show_and_speak_current_question(self):
        if not self.image_download_threads:
            self._display_current_question_options()
        else:
            self.status_label.setText("Aguardando download de imagem para recarregar pergunta...")
            self.status_label.show()