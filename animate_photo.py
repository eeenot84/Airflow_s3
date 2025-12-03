#!/usr/bin/env python3
"""
Скрипт для анимации фото и добавления звукового сообщения.
Оживляет изображение с эффектами и добавляет аудио с приветствием.
"""

import os
import sys
from pathlib import Path
from PIL import Image, ImageEnhance, ImageFilter
from moviepy.editor import ImageClip, AudioFileClip, CompositeVideoClip, concatenate_videoclips
from moviepy.video.fx.all import fadein, fadeout
from gtts import gTTS
import tempfile

def create_animated_photo(input_image_path, output_video_path, audio_text="У меня все хорошо. Всем привет!"):
    """
    Создает анимированное видео из фото с добавлением звука.
    
    Args:
        input_image_path: Путь к входному изображению
        output_video_path: Путь для сохранения выходного видео
        audio_text: Текст для озвучки (по умолчанию на русском)
    """
    
    # Проверяем наличие входного файла
    if not os.path.exists(input_image_path):
        print(f"Ошибка: Файл {input_image_path} не найден!")
        print("Пожалуйста, укажите путь к изображению.")
        return False
    
    print(f"Обработка изображения: {input_image_path}")
    
    # Загружаем изображение
    img = Image.open(input_image_path)
    
    # Создаем временную директорию для промежуточных файлов
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Создаем несколько кадров с эффектами анимации
        frames = []
        duration_per_frame = 0.5  # Длительность каждого эффекта в секундах
        total_duration = 3.0  # Общая длительность видео
        
        # Эффект 1: Исходное изображение с легким увеличением
        print("Создание анимации: эффект увеличения...")
        for i in range(6):
            scale = 1.0 + (i * 0.05)  # Постепенное увеличение на 5% за кадр
            width = int(img.width * scale)
            height = int(img.height * scale)
            resized = img.resize((width, height), Image.LANCZOS)
            
            # Обрезаем до исходного размера с центрированием
            left = (width - img.width) // 2
            top = (height - img.height) // 2
            cropped = resized.crop((left, top, left + img.width, top + img.height))
            
            frame_path = os.path.join(temp_dir, f"frame_zoom_{i:03d}.png")
            cropped.save(frame_path)
            frames.append(frame_path)
        
        # Эффект 2: Яркость и контраст
        print("Создание анимации: эффект яркости...")
        for i in range(6):
            enhancer = ImageEnhance.Brightness(img)
            brightness = 0.8 + (i * 0.04)  # От 80% до 104%
            bright_img = enhancer.enhance(brightness)
            
            enhancer_contrast = ImageEnhance.Contrast(bright_img)
            contrast = 0.9 + (i * 0.02)  # От 90% до 102%
            final_img = enhancer_contrast.enhance(contrast)
            
            frame_path = os.path.join(temp_dir, f"frame_bright_{i:03d}.png")
            final_img.save(frame_path)
            frames.append(frame_path)
        
        # Создаем видео клипы из кадров
        print("Создание видео клипов...")
        clips = []
        for i, frame_path in enumerate(frames):
            clip = ImageClip(frame_path, duration=duration_per_frame)
            # Применяем эффект fade in/out
            if i == 0:
                clip = fadein(clip, 0.2)
            if i == len(frames) - 1:
                clip = fadeout(clip, 0.2)
            clips.append(clip)
        
        # Объединяем клипы
        video = concatenate_videoclips(clips, method="compose")
        
        # Обрезаем до нужной длительности
        if video.duration > total_duration:
            video = video.subclip(0, total_duration)
        else:
            # Если видео короче, повторяем последний кадр
            last_frame = clips[-1]
            while video.duration < total_duration:
                video = concatenate_videoclips([video, last_frame.subclip(0, min(0.5, total_duration - video.duration))])
        
        # Создаем аудио с текстом
        print(f"Создание аудио: '{audio_text}'...")
        audio_path = os.path.join(temp_dir, "audio.mp3")
        tts = gTTS(text=audio_text, lang='ru', slow=False)
        tts.save(audio_path)
        
        # Загружаем аудио
        audio = AudioFileClip(audio_path)
        
        # Синхронизируем длительность видео с аудио
        if video.duration < audio.duration:
            # Увеличиваем длительность видео до длительности аудио
            last_frame = clips[-1]
            while video.duration < audio.duration:
                video = concatenate_videoclips([
                    video, 
                    last_frame.subclip(0, min(0.5, audio.duration - video.duration))
                ])
        else:
            # Обрезаем видео до длительности аудио
            video = video.subclip(0, audio.duration)
        
        # Объединяем видео и аудио
        print("Объединение видео и аудио...")
        final_video = video.set_audio(audio)
        
        # Сохраняем результат
        print(f"Сохранение видео: {output_video_path}")
        final_video.write_videofile(
            output_video_path,
            fps=24,
            codec='libx264',
            audio_codec='aac',
            temp_audiofile=os.path.join(temp_dir, "temp_audio.m4a"),
            remove_temp=True,
            verbose=False,
            logger=None
        )
        
        print(f"✅ Готово! Видео сохранено: {output_video_path}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при создании видео: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Очищаем временные файлы
        import shutil
        try:
            shutil.rmtree(temp_dir)
        except:
            pass


def main():
    """Главная функция"""
    if len(sys.argv) < 2:
        print("Использование: python animate_photo.py <путь_к_изображению> [выходной_файл]")
        print("\nПример:")
        print("  python animate_photo.py photo.jpg output.mp4")
        print("  python animate_photo.py photo.jpg")
        sys.exit(1)
    
    input_image = sys.argv[1]
    
    if len(sys.argv) >= 3:
        output_video = sys.argv[2]
    else:
        # Создаем имя выходного файла на основе входного
        input_path = Path(input_image)
        output_video = str(input_path.parent / f"{input_path.stem}_animated.mp4")
    
    # Текст для озвучки
    audio_text = "У меня все хорошо. Всем привет!"
    
    success = create_animated_photo(input_image, output_video, audio_text)
    
    if success:
        print(f"\n🎉 Видео успешно создано!")
        print(f"📁 Файл: {output_video}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
