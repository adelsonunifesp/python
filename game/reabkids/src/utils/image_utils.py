
from PIL import Image, ImageDraw, ImageFont
import os

def create_brazil_flag(image_data_dir, flag_size=(50, 50)):
    brazil_flag_path = os.path.join(image_data_dir, 'brazil_flag.png')
    if not os.path.exists(brazil_flag_path):
        try:
            flag = Image.new('RGB', flag_size, color=(0, 156, 59))
            draw = ImageDraw.Draw(flag)
            center_x, center_y = flag_size[0] / 2, flag_size[1] / 2
            lx, ly = flag_size[0] * 0.35, flag_size[1] * 0.35
            draw.polygon([
                (center_x - lx, center_y), (center_x, center_y - ly),
                (center_x + lx, center_y), (center_x, center_y + ly)
            ], fill=(255, 223, 0))
            circle_radius = flag_size[0] * 0.14
            draw.ellipse([
                center_x - circle_radius, center_y - circle_radius,
                center_x + circle_radius, center_y + circle_radius
            ], fill=(0, 39, 118))
            flag.save(brazil_flag_path)
        except Exception as e:
            print(f"Erro ao criar bandeira do Brasil: {e}. Certifique-se de que Pillow está instalada corretamente.")

def create_us_flag(image_data_dir, flag_size=(50, 50)):
    us_flag_path = os.path.join(image_data_dir, 'us_flag.png')
    if not os.path.exists(us_flag_path):
        try:
            flag = Image.new('RGB', flag_size, color=(255, 255, 255))
            draw = ImageDraw.Draw(flag)
            stripe_height = flag_size[1] / 13
            for i in range(13):
                if i % 2 == 0:
                    draw.rectangle([0, i * stripe_height, flag_size[0], (i + 1) * stripe_height], fill=(187, 19, 62))
            union_width_ratio = 0.4
            union_height_ratio = 7 / 13
            union_width = flag_size[0] * union_width_ratio
            union_height = flag_size[1] * union_height_ratio
            draw.rectangle([0, 0, union_width, union_height], fill=(0, 21, 99))
            star_size_px = 3
            try:
                star_font = ImageFont.truetype("arial.ttf", int(star_size_px * 2.5))
            except IOError:
                star_font = ImageFont.load_default()

            for r in range(5):
                for c in range(6):
                    x_pos = (c * (union_width / 6)) + (union_width / 12)
                    y_pos = (r * (union_height / 5)) + (union_height / 10)
                    draw.text((x_pos - star_size_px, y_pos - star_size_px), "*", font=star_font, fill='white')

            flag.save(us_flag_path)
        except Exception as e:
            print(f"Erro ao criar bandeira dos EUA: {e}. Certifique-se de que Pillow está instalada corretamente.")

def create_play_icon(image_data_dir, icon_size_menu=(30, 30)):
    play_icon_path = os.path.join(image_data_dir, 'play_icon.png')
    if not os.path.exists(play_icon_path):
        try:
            icon = Image.new('RGBA', icon_size_menu, (0, 0, 0, 0))
            draw_icon = ImageDraw.Draw(icon)
            draw_icon.polygon([(5, 5), (icon_size_menu[0] - 5, icon_size_menu[1] // 2), (5, icon_size_menu[1] - 5)],
                              fill='blue')
            icon.save(play_icon_path)
        except Exception as e:
            print(f"Erro ao criar play_icon.png: {e}")

def create_settings_icon(image_data_dir, icon_size_menu=(30, 30)):
    settings_icon_path = os.path.join(image_data_dir, 'settings_icon.png')
    if not os.path.exists(settings_icon_path):
        try:
            icon = Image.new('RGBA', icon_size_menu, (0, 0, 0, 0))
            draw_icon = ImageDraw.Draw(icon)
            draw_icon.ellipse([7, 7, 23, 23], fill='gray')
            draw_icon.rectangle([13, 0, 17, 7], fill='gray')
            draw_icon.rectangle([13, 23, 17, 30], fill='gray')
            draw_icon.rectangle([0, 13, 7, 17], fill='gray')
            draw_icon.rectangle([23, 13, 30, 17], fill='gray')
            icon.save(settings_icon_path)
        except Exception as e:
            print(f"Erro ao criar settings_icon.png: {e}")

def create_exit_icon(image_data_dir, icon_size_menu=(30, 30)):
    exit_icon_path = os.path.join(image_data_dir, 'exit_icon.png')
    if not os.path.exists(exit_icon_path):
        try:
            icon = Image.new('RGBA', icon_size_menu, (0, 0, 0, 0))
            draw_icon = ImageDraw.Draw(icon)
            draw_icon.line([icon_size_menu[0] // 2, 5, icon_size_menu[0] // 2, icon_size_menu[1] - 5], fill='red',
                           width=3)
            draw_icon.polygon([
                (icon_size_menu[0] // 2, 5),
                (icon_size_menu[0] // 2 - 5, 10),
                (icon_size_menu[0] // 2 + 5, 10)
            ], fill='red')
            draw_icon.rectangle([5, 10, icon_size_menu[0] - 5, icon_size_menu[1] - 5], outline='red', width=2)
            icon.save(exit_icon_path)
        except Exception as e:
            print(f"Erro ao criar exit_icon.png: {e}")

def create_arrow_right_icon(image_data_dir, icon_size_menu=(30, 30)):
    arrow_right_icon_path = os.path.join(image_data_dir, 'arrow_right_icon.png')
    if not os.path.exists(arrow_right_icon_path):
        try:
            icon = Image.new('RGBA', icon_size_menu, (0, 0, 0, 0))
            draw_icon = ImageDraw.Draw(icon)
            draw_icon.polygon([(10, 5), (25, 15), (10, 25)], fill='blue')
            icon.save(arrow_right_icon_path)
        except Exception as e:
            print(f"Erro ao criar arrow_right_icon.png: {e}")

def create_all_assets(image_data_dir):
    create_brazil_flag(image_data_dir)
    create_us_flag(image_data_dir)
    create_play_icon(image_data_dir)
    create_settings_icon(image_data_dir)
    create_exit_icon(image_data_dir)
    create_arrow_right_icon(image_data_dir)
