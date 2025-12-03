#!/usr/bin/env python3
"""
Скрипт для оживления фото - создание анимации из статического изображения
Поддерживает различные эффекты: Ken Burns, дрожание, масштабирование, вращение
"""

import argparse
from PIL import Image, ImageEnhance, ImageFilter
import os
from pathlib import Path


def ken_burns_effect(image, frames=30, zoom_factor=1.2, pan_x=0.1, pan_y=0.1):
    """
    Эффект Ken Burns - плавное панорамирование и масштабирование
    """
    width, height = image.size
    frames_list = []
    
    for i in range(frames):
        # Вычисляем текущий масштаб (от 1.0 до zoom_factor)
        scale = 1.0 + (zoom_factor - 1.0) * (i / frames)
        
        # Вычисляем смещение для панорамирования
        offset_x = int((width * scale - width) * pan_x * (i / frames))
        offset_y = int((height * scale - height) * pan_y * (i / frames))
        
        # Создаем новое изображение с нужным размером
        new_width = int(width * scale)
        new_height = int(height * scale)
        scaled_img = image.resize((new_width, new_height), Image.LANCZOS)
        
        # Обрезаем до исходного размера с учетом смещения
        crop_box = (
            offset_x,
            offset_y,
            offset_x + width,
            offset_y + height
        )
        
        # Убеждаемся, что координаты не выходят за границы
        crop_box = (
            max(0, crop_box[0]),
            max(0, crop_box[1]),
            min(new_width, crop_box[2]),
            min(new_height, crop_box[3])
        )
        
        cropped = scaled_img.crop(crop_box)
        
        # Если размер не совпадает, создаем новое изображение и вставляем
        if cropped.size != (width, height):
            final_img = Image.new('RGB', (width, height), (0, 0, 0))
            final_img.paste(cropped, (0, 0))
            frames_list.append(final_img)
        else:
            frames_list.append(cropped)
    
    return frames_list


def zoom_effect(image, frames=30, zoom_factor=1.3):
    """
    Эффект плавного приближения
    """
    width, height = image.size
    frames_list = []
    
    for i in range(frames):
        scale = 1.0 + (zoom_factor - 1.0) * (i / frames)
        new_width = int(width * scale)
        new_height = int(height * scale)
        
        scaled_img = image.resize((new_width, new_height), Image.LANCZOS)
        
        # Центрируем обрезку
        left = (new_width - width) // 2
        top = (new_height - height) // 2
        right = left + width
        bottom = top + height
        
        cropped = scaled_img.crop((left, top, right, bottom))
        frames_list.append(cropped)
    
    return frames_list


def shake_effect(image, frames=30, intensity=5):
    """
    Эффект дрожания камеры
    """
    import random
    frames_list = []
    width, height = image.size
    
    for i in range(frames):
        # Случайное смещение
        offset_x = random.randint(-intensity, intensity)
        offset_y = random.randint(-intensity, intensity)
        
        # Создаем новое изображение и вставляем оригинал со смещением
        new_img = Image.new('RGB', (width, height), (0, 0, 0))
        paste_x = max(0, offset_x)
        paste_y = max(0, offset_y)
        crop_x = max(0, -offset_x)
        crop_y = max(0, -offset_y)
        crop_width = width - abs(offset_x)
        crop_height = height - abs(offset_y)
        
        cropped = image.crop((crop_x, crop_y, crop_x + crop_width, crop_y + crop_height))
        new_img.paste(cropped, (paste_x, paste_y))
        frames_list.append(new_img)
    
    return frames_list


def rotate_effect(image, frames=30, rotation_degrees=10):
    """
    Эффект плавного вращения
    """
    frames_list = []
    
    for i in range(frames):
        angle = (rotation_degrees * 2 / frames) * (i - frames / 2)
        rotated = image.rotate(angle, expand=False, fillcolor=(0, 0, 0))
        frames_list.append(rotated)
    
    return frames_list


def fade_effect(image, frames=30):
    """
    Эффект плавного появления/исчезновения
    """
    frames_list = []
    
    for i in range(frames):
        alpha = int(255 * (i / frames))
        # Создаем копию изображения
        frame = image.copy()
        # Применяем прозрачность через альфа-канал
        if frame.mode != 'RGBA':
            frame = frame.convert('RGBA')
        
        # Создаем маску прозрачности
        alpha_channel = Image.new('L', frame.size, alpha)
        frame.putalpha(alpha_channel)
        
        # Конвертируем обратно в RGB для GIF
        rgb_frame = Image.new('RGB', frame.size, (0, 0, 0))
        rgb_frame.paste(frame, mask=frame.split()[3])
        frames_list.append(rgb_frame)
    
    return frames_list


def brightness_pulse(image, frames=30, pulse_factor=0.3):
    """
    Эффект пульсации яркости
    """
    frames_list = []
    
    for i in range(frames):
        # Синусоидальная функция для плавной пульсации
        import math
        brightness = 1.0 + pulse_factor * math.sin(2 * math.pi * i / frames)
        enhancer = ImageEnhance.Brightness(image)
        enhanced = enhancer.enhance(brightness)
        frames_list.append(enhanced)
    
    return frames_list


def animate_photo(input_path, output_path, effect='ken_burns', duration=2000, 
                 frames=30, **kwargs):
    """
    Создает анимацию из фото с выбранным эффектом
    
    Args:
        input_path: путь к исходному изображению
        output_path: путь для сохранения GIF
        effect: тип эффекта ('ken_burns', 'zoom', 'shake', 'rotate', 'fade', 'brightness')
        duration: длительность анимации в миллисекундах
        frames: количество кадров
        **kwargs: дополнительные параметры для эффектов
    """
    # Загружаем изображение
    try:
        image = Image.open(input_path)
        # Конвертируем в RGB если нужно
        if image.mode != 'RGB':
            image = image.convert('RGB')
    except Exception as e:
        print(f"❌ Ошибка при загрузке изображения: {e}")
        return False
    
    print(f"📸 Загружено изображение: {image.size[0]}x{image.size[1]}")
    
    # Выбираем эффект
    effects = {
        'ken_burns': ken_burns_effect,
        'zoom': zoom_effect,
        'shake': shake_effect,
        'rotate': rotate_effect,
        'fade': fade_effect,
        'brightness': brightness_pulse,
    }
    
    if effect not in effects:
        print(f"❌ Неизвестный эффект: {effect}")
        print(f"Доступные эффекты: {', '.join(effects.keys())}")
        return False
    
    print(f"🎬 Применяем эффект: {effect}")
    
    # Генерируем кадры
    effect_func = effects[effect]
    frames_list = effect_func(image, frames=frames, **kwargs)
    
    # Вычисляем задержку между кадрами
    delay = duration // frames
    
    # Сохраняем как GIF
    try:
        frames_list[0].save(
            output_path,
            save_all=True,
            append_images=frames_list[1:],
            duration=delay,
            loop=0,
            optimize=True
        )
        print(f"✅ Анимация сохранена: {output_path}")
        print(f"📊 Параметры: {frames} кадров, {delay}мс на кадр, общая длительность ~{duration}мс")
        return True
    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Оживление фото - создание анимации из статического изображения',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Эффект Ken Burns (по умолчанию)
  python animate_photo.py photo.jpg output.gif

  # Эффект приближения
  python animate_photo.py photo.jpg output.gif --effect zoom --zoom-factor 1.5

  # Эффект дрожания
  python animate_photo.py photo.jpg output.gif --effect shake --intensity 10

  # Эффект вращения
  python animate_photo.py photo.jpg output.gif --effect rotate --rotation-degrees 20

  # Эффект пульсации яркости
  python animate_photo.py photo.jpg output.gif --effect brightness --pulse-factor 0.5

  # Эффект появления
  python animate_photo.py photo.jpg output.gif --effect fade --frames 60
        """
    )
    
    parser.add_argument('input', help='Путь к исходному изображению')
    parser.add_argument('output', help='Путь для сохранения анимированного GIF')
    parser.add_argument('--effect', '-e', 
                       choices=['ken_burns', 'zoom', 'shake', 'rotate', 'fade', 'brightness'],
                       default='ken_burns',
                       help='Тип эффекта (по умолчанию: ken_burns)')
    parser.add_argument('--frames', '-f', type=int, default=30,
                       help='Количество кадров (по умолчанию: 30)')
    parser.add_argument('--duration', '-d', type=int, default=2000,
                       help='Длительность анимации в миллисекундах (по умолчанию: 2000)')
    
    # Параметры для различных эффектов
    parser.add_argument('--zoom-factor', type=float, default=1.2,
                       help='Коэффициент масштабирования для zoom/ken_burns (по умолчанию: 1.2)')
    parser.add_argument('--pan-x', type=float, default=0.1,
                       help='Горизонтальное панорамирование для ken_burns (по умолчанию: 0.1)')
    parser.add_argument('--pan-y', type=float, default=0.1,
                       help='Вертикальное панорамирование для ken_burns (по умолчанию: 0.1)')
    parser.add_argument('--intensity', type=int, default=5,
                       help='Интенсивность дрожания для shake (по умолчанию: 5)')
    parser.add_argument('--rotation-degrees', type=float, default=10,
                       help='Угол вращения для rotate (по умолчанию: 10)')
    parser.add_argument('--pulse-factor', type=float, default=0.3,
                       help='Фактор пульсации для brightness (по умолчанию: 0.3)')
    
    args = parser.parse_args()
    
    # Проверяем существование входного файла
    if not os.path.exists(args.input):
        print(f"❌ Файл не найден: {args.input}")
        return 1
    
    # Подготавливаем параметры для эффекта
    effect_kwargs = {}
    if args.effect in ['ken_burns', 'zoom']:
        effect_kwargs['zoom_factor'] = args.zoom_factor
    if args.effect == 'ken_burns':
        effect_kwargs['pan_x'] = args.pan_x
        effect_kwargs['pan_y'] = args.pan_y
    elif args.effect == 'shake':
        effect_kwargs['intensity'] = args.intensity
    elif args.effect == 'rotate':
        effect_kwargs['rotation_degrees'] = args.rotation_degrees
    elif args.effect == 'brightness':
        effect_kwargs['pulse_factor'] = args.pulse_factor
    
    # Создаем анимацию
    success = animate_photo(
        args.input,
        args.output,
        effect=args.effect,
        duration=args.duration,
        frames=args.frames,
        **effect_kwargs
    )
    
    return 0 if success else 1


if __name__ == '__main__':
    exit(main())
