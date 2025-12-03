#!/usr/bin/env python3
"""
Пример использования модуля анимации фотографий.
Использование:
    python example_animate.py path/to/photo.jpg output.gif
"""
import sys
import os
from photo_animator import PhotoAnimator

def main():
    if len(sys.argv) < 3:
        print("Использование: python example_animate.py <input_photo> <output_gif>")
        print("\nПримеры:")
        print("  python example_animate.py photo.jpg animated.gif")
        print("  python example_animate.py photo.jpg animated.gif --effect zoom")
        print("  python example_animate.py photo.jpg animated.gif --effect pan --direction right")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Параметры по умолчанию
    effect = 'ken_burns'
    direction = 'right'
    
    # Парсинг аргументов
    if '--effect' in sys.argv:
        idx = sys.argv.index('--effect')
        if idx + 1 < len(sys.argv):
            effect = sys.argv[idx + 1]
    
    if '--direction' in sys.argv:
        idx = sys.argv.index('--direction')
        if idx + 1 < len(sys.argv):
            direction = sys.argv[idx + 1]
    
    if not os.path.exists(input_path):
        print(f"❌ Файл не найден: {input_path}")
        sys.exit(1)
    
    print(f"🎬 Создание анимации из {input_path}")
    print(f"   Эффект: {effect}")
    print(f"   Выходной файл: {output_path}")
    
    try:
        animator = PhotoAnimator(output_format='gif')
        
        animator.animate_photo(
            image_path=input_path,
            output_path=output_path,
            effect=effect,
            frames=60,
            zoom_factor=1.3,
            pan_direction=direction,
            duration=3.0
        )
        
        print(f"✅ Анимация успешно создана: {output_path}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
