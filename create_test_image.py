#!/usr/bin/env python3
"""Создает тестовое изображение для демонстрации"""
from PIL import Image, ImageDraw, ImageFont
import os

def create_test_image(output_path="test_photo.jpg"):
    """Создает красивое тестовое изображение"""
    width, height = 800, 600
    img = Image.new('RGB', (width, height), color='#4A90E2')
    draw = ImageDraw.Draw(img)
    
    # Рисуем градиентный фон
    for y in range(height):
        r = int(74 + (y / height) * 30)
        g = int(144 + (y / height) * 20)
        b = int(226 + (y / height) * 20)
        draw.rectangle([(0, y), (width, y+1)], fill=(r, g, b))
    
    # Рисуем солнце
    sun_center = (width - 150, 100)
    sun_radius = 80
    draw.ellipse(
        [sun_center[0] - sun_radius, sun_center[1] - sun_radius,
         sun_center[0] + sun_radius, sun_center[1] + sun_radius],
        fill='#FFD700'
    )
    
    # Рисуем облака
    cloud_positions = [(200, 150), (400, 120), (600, 180)]
    for x, y in cloud_positions:
        draw.ellipse([x-30, y, x+30, y+30], fill='white')
        draw.ellipse([x-10, y-10, x+50, y+20], fill='white')
        draw.ellipse([x+10, y, x+70, y+30], fill='white')
    
    # Рисуем текст
    try:
        # Пытаемся использовать системный шрифт
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 48)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 32)
    except:
        # Если системный шрифт недоступен, используем стандартный
        font_large = ImageFont.load_default()
        font_small = ImageFont.load_default()
    
    text1 = "Привет!"
    text2 = "Все отлично!"
    
    # Получаем размеры текста
    bbox1 = draw.textbbox((0, 0), text1, font=font_large)
    bbox2 = draw.textbbox((0, 0), text2, font=font_small)
    
    text1_width = bbox1[2] - bbox1[0]
    text2_width = bbox2[2] - bbox2[0]
    
    # Рисуем текст с тенью
    shadow_offset = 3
    draw.text(((width - text1_width) // 2 + shadow_offset, height // 2 - 40 + shadow_offset), 
              text1, fill='black', font=font_large)
    draw.text(((width - text1_width) // 2, height // 2 - 40), 
              text1, fill='white', font=font_large)
    
    draw.text(((width - text2_width) // 2 + shadow_offset, height // 2 + 20 + shadow_offset), 
              text2, fill='black', font=font_small)
    draw.text(((width - text2_width) // 2, height // 2 + 20), 
              text2, fill='white', font=font_small)
    
    # Сохраняем изображение
    img.save(output_path, quality=95)
    print(f"✅ Тестовое изображение создано: {output_path}")
    return output_path

if __name__ == "__main__":
    create_test_image()
