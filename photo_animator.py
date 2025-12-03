"""
Модуль для анимации статичных фотографий.
Создает анимированные GIF или видео из статичных изображений.
"""
import logging
import os
from typing import Optional, Tuple
from io import BytesIO
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter
import imageio
import cv2

logger = logging.getLogger(__name__)


class PhotoAnimator:
    """Класс для создания анимаций из статичных фотографий."""
    
    def __init__(self, output_format: str = 'gif'):
        """
        Инициализация аниматора.
        
        Args:
            output_format: Формат выходного файла ('gif' или 'mp4')
        """
        self.output_format = output_format.lower()
        if self.output_format not in ['gif', 'mp4']:
            raise ValueError("output_format должен быть 'gif' или 'mp4'")
    
    def load_image(self, image_path: str) -> Image.Image:
        """
        Загружает изображение из файла или байтов.
        
        Args:
            image_path: Путь к изображению или байты изображения
            
        Returns:
            PIL Image объект
        """
        if isinstance(image_path, bytes):
            return Image.open(BytesIO(image_path))
        return Image.open(image_path)
    
    def create_zoom_animation(
        self,
        image: Image.Image,
        frames: int = 30,
        zoom_factor: float = 1.2,
        duration: float = 2.0
    ) -> list:
        """
        Создает анимацию приближения (zoom in).
        
        Args:
            image: Исходное изображение
            frames: Количество кадров
            zoom_factor: Коэффициент увеличения (1.0 = без изменений)
            duration: Длительность анимации в секундах
            
        Returns:
            Список кадров изображений
        """
        frames_list = []
        width, height = image.size
        
        for i in range(frames):
            # Вычисляем текущий масштаб
            scale = 1.0 + (zoom_factor - 1.0) * (i / frames)
            
            # Вычисляем новые размеры
            new_width = int(width * scale)
            new_height = int(height * scale)
            
            # Масштабируем изображение
            scaled = image.resize((new_width, new_height), Image.LANCZOS)
            
            # Обрезаем до исходного размера (центрируем)
            left = (new_width - width) // 2
            top = (new_height - height) // 2
            cropped = scaled.crop((left, top, left + width, top + height))
            
            frames_list.append(cropped)
        
        return frames_list
    
    def create_pan_animation(
        self,
        image: Image.Image,
        frames: int = 30,
        direction: str = 'right',
        pan_distance: float = 0.3,
        duration: float = 2.0
    ) -> list:
        """
        Создает анимацию панорамирования (движения камеры).
        
        Args:
            image: Исходное изображение
            frames: Количество кадров
            direction: Направление движения ('left', 'right', 'up', 'down')
            pan_distance: Расстояние панорамирования (0.0-1.0)
            duration: Длительность анимации в секундах
            
        Returns:
            Список кадров изображений
        """
        frames_list = []
        width, height = image.size
        
        # Создаем увеличенное изображение для панорамирования
        zoom = 1.2
        enlarged_width = int(width * zoom)
        enlarged_height = int(height * zoom)
        enlarged = image.resize((enlarged_width, enlarged_height), Image.LANCZOS)
        
        # Определяем направление движения
        max_x = int((enlarged_width - width) * pan_distance)
        max_y = int((enlarged_height - height) * pan_distance)
        
        for i in range(frames):
            progress = i / frames
            
            if direction == 'right':
                x_offset = int(max_x * progress)
                y_offset = 0
            elif direction == 'left':
                x_offset = int(max_x * (1 - progress))
                y_offset = 0
            elif direction == 'down':
                x_offset = 0
                y_offset = int(max_y * progress)
            elif direction == 'up':
                x_offset = 0
                y_offset = int(max_y * (1 - progress))
            else:
                x_offset = 0
                y_offset = 0
            
            # Обрезаем область
            cropped = enlarged.crop((
                x_offset, y_offset,
                x_offset + width, y_offset + height
            ))
            
            frames_list.append(cropped)
        
        return frames_list
    
    def create_ken_burns_effect(
        self,
        image: Image.Image,
        frames: int = 60,
        zoom_factor: float = 1.3,
        pan_direction: str = 'right',
        duration: float = 3.0
    ) -> list:
        """
        Создает эффект Кена Бернса (комбинация zoom и pan).
        
        Args:
            image: Исходное изображение
            frames: Количество кадров
            zoom_factor: Коэффициент увеличения
            pan_direction: Направление панорамирования
            duration: Длительность анимации в секундах
            
        Returns:
            Список кадров изображений
        """
        frames_list = []
        width, height = image.size
        
        # Создаем увеличенное изображение
        enlarged_width = int(width * zoom_factor)
        enlarged_height = int(height * zoom_factor)
        enlarged = image.resize((enlarged_width, enlarged_height), Image.LANCZOS)
        
        # Вычисляем максимальное смещение
        max_x = enlarged_width - width
        max_y = enlarged_height - height
        
        for i in range(frames):
            progress = i / frames
            
            # Комбинируем zoom и pan
            current_zoom = 1.0 + (zoom_factor - 1.0) * progress
            
            if pan_direction == 'right':
                x_offset = int(max_x * progress * 0.5)
                y_offset = int(max_y * 0.3)
            elif pan_direction == 'left':
                x_offset = int(max_x * (1 - progress) * 0.5)
                y_offset = int(max_y * 0.3)
            else:
                x_offset = int(max_x * 0.2)
                y_offset = int(max_y * progress * 0.5)
            
            # Обрезаем область
            cropped = enlarged.crop((
                x_offset, y_offset,
                x_offset + width, y_offset + height
            ))
            
            frames_list.append(cropped)
        
        return frames_list
    
    def create_fade_animation(
        self,
        image: Image.Image,
        frames: int = 30,
        fade_type: str = 'in_out',
        duration: float = 2.0
    ) -> list:
        """
        Создает анимацию затухания/появления.
        
        Args:
            image: Исходное изображение
            frames: Количество кадров
            fade_type: Тип эффекта ('in', 'out', 'in_out')
            duration: Длительность анимации в секундах
            
        Returns:
            Список кадров изображений
        """
        frames_list = []
        
        for i in range(frames):
            progress = i / frames
            
            if fade_type == 'in':
                alpha = progress
            elif fade_type == 'out':
                alpha = 1.0 - progress
            elif fade_type == 'in_out':
                if progress < 0.5:
                    alpha = progress * 2
                else:
                    alpha = 2.0 - (progress * 2)
            else:
                alpha = 1.0
            
            # Создаем копию изображения с измененной прозрачностью
            frame = image.copy()
            if frame.mode != 'RGBA':
                frame = frame.convert('RGBA')
            
            # Применяем альфа-канал
            alpha_channel = frame.split()[3]
            alpha_channel = alpha_channel.point(lambda p: int(p * alpha))
            frame.putalpha(alpha_channel)
            
            frames_list.append(frame)
        
        return frames_list
    
    def save_animation(
        self,
        frames: list,
        output_path: str,
        duration: Optional[float] = None,
        loop: int = 0
    ) -> str:
        """
        Сохраняет анимацию в файл.
        
        Args:
            frames: Список кадров изображений
            output_path: Путь для сохранения
            duration: Длительность одного кадра в секундах (для GIF)
            loop: Количество циклов (0 = бесконечно)
            
        Returns:
            Путь к сохраненному файлу
        """
        if not frames:
            raise ValueError("Список кадров пуст")
        
        if duration is None:
            duration = 0.1  # По умолчанию 0.1 секунды на кадр
        
        if self.output_format == 'gif':
            # Сохраняем как GIF
            frames[0].save(
                output_path,
                save_all=True,
                append_images=frames[1:],
                duration=int(duration * 1000),  # В миллисекундах
                loop=loop,
                optimize=True
            )
        elif self.output_format == 'mp4':
            # Сохраняем как MP4
            fps = int(1.0 / duration)
            writer = imageio.get_writer(
                output_path,
                fps=fps,
                codec='libx264',
                quality=8
            )
            
            for frame in frames:
                # Конвертируем PIL Image в numpy array
                if frame.mode == 'RGBA':
                    # Конвертируем RGBA в RGB для видео
                    rgb_frame = Image.new('RGB', frame.size, (255, 255, 255))
                    rgb_frame.paste(frame, mask=frame.split()[3])
                    frame = rgb_frame
                
                frame_array = np.array(frame)
                writer.append_data(frame_array)
            
            writer.close()
        
        logger.info(f"✅ Анимация сохранена: {output_path}")
        return output_path
    
    def animate_photo(
        self,
        image_path: str,
        output_path: str,
        effect: str = 'ken_burns',
        **kwargs
    ) -> str:
        """
        Главный метод для создания анимации из фото.
        
        Args:
            image_path: Путь к исходному изображению или байты
            output_path: Путь для сохранения анимации
            effect: Тип эффекта ('zoom', 'pan', 'ken_burns', 'fade')
            **kwargs: Дополнительные параметры для эффектов
            
        Returns:
            Путь к сохраненному файлу
        """
        logger.info(f"🎬 Создание анимации с эффектом: {effect}")
        
        # Загружаем изображение
        image = self.load_image(image_path)
        logger.info(f"📸 Изображение загружено: {image.size}")
        
        # Выбираем эффект
        frames_count = kwargs.get('frames', 60)
        
        if effect == 'zoom':
            frames = self.create_zoom_animation(
                image,
                frames=frames_count,
                zoom_factor=kwargs.get('zoom_factor', 1.2),
                duration=kwargs.get('duration', 2.0)
            )
        elif effect == 'pan':
            frames = self.create_pan_animation(
                image,
                frames=frames_count,
                direction=kwargs.get('direction', 'right'),
                pan_distance=kwargs.get('pan_distance', 0.3),
                duration=kwargs.get('duration', 2.0)
            )
        elif effect == 'ken_burns':
            frames = self.create_ken_burns_effect(
                image,
                frames=frames_count,
                zoom_factor=kwargs.get('zoom_factor', 1.3),
                pan_direction=kwargs.get('pan_direction', 'right'),
                duration=kwargs.get('duration', 3.0)
            )
        elif effect == 'fade':
            frames = self.create_fade_animation(
                image,
                frames=frames_count,
                fade_type=kwargs.get('fade_type', 'in_out'),
                duration=kwargs.get('duration', 2.0)
            )
        else:
            raise ValueError(f"Неизвестный эффект: {effect}")
        
        # Сохраняем анимацию
        duration_per_frame = kwargs.get('duration', 2.0) / frames_count
        return self.save_animation(
            frames,
            output_path,
            duration=duration_per_frame,
            loop=kwargs.get('loop', 0)
        )
