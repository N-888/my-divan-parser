"""
ИСПРАВЛЕННАЯ ВЕРСИЯ ПАУКА - СОХРАНЕНИЕ РАБОТАЕТ 100%
"""

import scrapy
import re
import csv
import os
from pathlib import Path


class FixedDivanSpider(scrapy.Spider):
    name = "fixed_divan"
    allowed_domains = ["divan.ru"]
    start_urls = ["https://www.divan.ru/category/svet"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # АБСОЛЮТНЫЙ путь к папке проекта
        self.project_path = Path(__file__).parent.parent.parent
        self.data_path = self.project_path / "data"
        self.data_path.mkdir(exist_ok=True)
        self.csv_path = self.data_path / "divan_products_FIXED.csv"

        self.parsed_data = []

        print("=" * 60)
        print("🔄 ИНИЦИАЛИЗАЦИЯ ПАУКА")
        print(f"📁 Проект: {self.project_path}")
        print(f"💾 CSV будет сохранен: {self.csv_path}")
        print("=" * 60)

    def parse(self, response, **kwargs):
        product_cards = response.css('div[data-testid="product-card"]')
        cards_list = list(product_cards)

        self.logger.info(f"🎯 Найдено карточек товаров: {len(cards_list)}")

        for card in cards_list:
            item_data = self.extract_item_data(card)
            if item_data:
                cleaned_data = self.clean_and_process_data(item_data)
                if cleaned_data:
                    self.parsed_data.append(cleaned_data)
                    yield cleaned_data

        # ✅ СОХРАНЕНИЕ СРАЗУ ПОСЛЕ ПАРСИНГА!
        self.save_to_csv()

    def extract_item_data(self, card):
        price_element = card.css('[data-testid="price"]::text')
        raw_price = price_element.get() if price_element else None

        url_element = card.css('a::attr(href)')
        raw_url = url_element.get() if url_element else None

        if raw_url and not raw_url.startswith('http'):
            raw_url = 'https://www.divan.ru' + raw_url

        raw_name = "Неизвестно"
        if raw_url:
            name_from_url = self.extract_name_from_url(raw_url)
            if name_from_url != "Неизвестно":
                raw_name = name_from_url

        if raw_name == "Неизвестно":
            name_from_card = self.extract_name_from_card(card)
            if name_from_card != "Неизвестно":
                raw_name = name_from_card

        return {
            'raw_name': raw_name,
            'raw_price': raw_price,
            'raw_url': raw_url
        }

    def extract_name_from_url(self, url):
        try:
            match = re.search(r'/product/([^/?]+)', url)
            if match:
                product_slug = match.group(1)
                name = product_slug.replace('-', ' ').title()
                return name
        except Exception as error:
            self.logger.warning(f"Ошибка при извлечении названия из URL: {error}")
        return "Неизвестно"

    def extract_name_from_card(self, card):
        try:
            all_text_elements = card.css('::text')
            all_texts = [text.get().strip() for text in all_text_elements if text.get().strip()]

            excluded_texts = ['Купить', 'NEW', 'В наличии', 'Размеры (ДхШхВ)', 'Размеры (ДхШхВ), см']

            meaningful_texts = []
            for text in all_texts:
                if 'руб' in text.lower():
                    continue
                if text in excluded_texts:
                    continue
                if re.search(r'\d+x\d+x\d+', text):
                    continue
                if len(text) < 10:
                    continue
                meaningful_texts.append(text)

            if meaningful_texts:
                best_name = max(meaningful_texts, key=len)
                return best_name
            else:
                return "Неизвестно"

        except Exception as error:
            self.logger.warning(f"Ошибка при извлечении названия из карточки: {error}")
            return "Неизвестно"

    def clean_and_process_data(self, item_data):
        cleaned_name = item_data['raw_name'].strip() if item_data['raw_name'] else "Неизвестно"
        cleaned_price = item_data['raw_price'].strip() if item_data['raw_price'] else "Цена не указана"
        cleaned_url = item_data['raw_url'] if item_data['raw_url'] else "Ссылка не найдена"

        cleaned_price = re.sub(r'[^\d\s]', '', cleaned_price).strip()

        try:
            price_number = int(cleaned_price.replace(' ', '')) if cleaned_price.replace(' ', '').isdigit() else 0
        except (ValueError, AttributeError):
            price_number = 0

        if price_number < 1000:
            return None

        processed_item = {
            'название': cleaned_name,
            'цена_руб': price_number,
            'цена_отформатированная': f"{price_number:,} руб.".replace(',', ' '),
            'ссылка': cleaned_url,
            'категория': 'Источники освещения'
        }

        return processed_item

    def save_to_csv(self):
        """СОХРАНЕНИЕ В CSV - ТЕПЕРЬ СРАБОТАЕТ 100%"""
        if not self.parsed_data:
            self.logger.info("❌ Нет данных для сохранения")
            return

        try:
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)

                writer.writerow([
                    'Название товара',
                    'Цена (руб)',
                    'Цена отформатированная',
                    'Ссылка на товар',
                    'Категория'
                ])

                for item in self.parsed_data:
                    writer.writerow([
                        item['название'],
                        item['цена_руб'],
                        item['цена_отформатированная'],
                        item['ссылка'],
                        item['категория']
                    ])

            # ✅ ВАЖНО: Выводим в консоль где файл
            print("=" * 60)
            print("💾 ФАЙЛ УСПЕШНО СОХРАНЕН!")
            print(f"📁 ПУТЬ: {self.csv_path}")
            print(f"📊 ТОВАРОВ: {len(self.parsed_data)}")
            print("=" * 60)

            # Дополнительная проверка что файл создан
            if os.path.exists(self.csv_path):
                file_size = os.path.getsize(self.csv_path)
                print(f"✅ Файл существует, размер: {file_size} байт")
            else:
                print("❌ Файл не создан!")

        except Exception as error:
            print(f"❌ ОШИБКА СОХРАНЕНИЯ: {error}")

    def closed(self, reason):
        """ДУБЛИРУЕМ СОХРАНЕНИЕ НА ВСЯКИЙ СЛУЧАЙ"""
        print(f"🔄 Паук завершает работу: {reason}")
        self.save_to_csv()