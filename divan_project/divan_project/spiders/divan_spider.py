"""
ПАУК ДЛЯ ПАРСИНГА САЙТА DIVAN.RU
Этот файл содержит всю логику сбора и обработки данных
"""

# Импортируем библиотеки - это как взять инструменты из ящика
import scrapy  # главный инструмент для парсинга
import re  # для поиска и замены текста
import csv  # для сохранения в CSV формат
import os  # для работы с папками и файлами


# Создаем класс нашего паука (как создать нового работника)
class DivanLightingSpider(scrapy.Spider):
    # Даем имя нашему пауку - как зовут работника
    name = "divan_lighting"

    # Говорим, какие сайты можно посещать (чтобы не пошел куда не надо)
    allowed_domains = ["divan.ru"]

    # С какого адреса начинать работу
    start_urls = ["https://www.divan.ru/category/svet"]

    # Этот метод запускается при создании паука (как инструкция при приеме на работу)
    def __init__(self, *args, **kwargs):
        # Вызываем инструкцию от начальника (родительского класса)
        super().__init__(*args, **kwargs)
        # Создаем папку 'data' для результатов (если нет - создаем, если есть - не ругаемся)
        os.makedirs('data', exist_ok=True)
        # Создаем пустой список, куда будем складывать все найденные товары
        self.parsed_data = []

    # Главный метод - здесь происходит весь парсинг
    def parse(self, response, **kwargs):
        """
        response - это страница сайта, которую мы получили
        """

        # Ищем все карточки товаров на странице (как находим все товары на полке)
        # Ищем по специальному атрибуту data-testid="product-card"
        product_cards = response.css('div[data-testid="product-card"]')

        # Превращаем результат в обычный список (для удобства)
        cards_list = list(product_cards)

        # Пишем в консоль сколько нашли товаров (для информации)
        self.logger.info(f"Найдено карточек товаров: {len(cards_list)}")

        # Теперь проходим по КАЖДОЙ карточке товара (как смотрим каждый товар на полке)
        for card in cards_list:
            # Извлекаем данные из карточки (смотрим что за товар)
            item_data = self.extract_item_data(card)

            # Если данные получили (товар не пустой)
            if item_data:
                # Очищаем и обрабатываем данные (приводим в порядок)
                cleaned_data = self.clean_and_process_data(item_data)

                # Если данные прошли очистку (товар нам подходит)
                if cleaned_data:
                    # Добавляем товар в общий список
                    self.parsed_data.append(cleaned_data)
                    # Возвращаем данные (скармливаем их Scrapy)
                    yield cleaned_data

        # После обработки всех товаров сохраняем в CSV
        self.save_to_csv()

    # Метод для извлечения данных из одной карточки товара
    def extract_item_data(self, card):
        """
        card - одна карточка товара (как один товар на полке)
        """

        # Ищем цену в карточке
        price_element = card.css('[data-testid="price"]::text')
        # Если нашли цену - берем текст, если нет - ставим None (ничего)
        raw_price = price_element.get() if price_element else None

        # Ищем ссылку на товар
        url_element = card.css('a::attr(href)')
        raw_url = url_element.get() if url_element else None

        # Если ссылка не полная (без https), делаем ее полной
        if raw_url and not raw_url.startswith('http'):
            raw_url = 'https://www.divan.ru' + raw_url

        # Пытаемся найти название товара
        raw_name = "Неизвестно"  # на случай если не найдем

        # Сначала пробуем извлечь название из ссылки (там часто есть название)
        if raw_url:
            name_from_url = self.extract_name_from_url(raw_url)
            if name_from_url != "Неизвестно":
                raw_name = name_from_url

        # Если из ссылки не получилось, пробуем из карточки
        if raw_name == "Неизвестно":
            name_from_card = self.extract_name_from_card(card)
            if name_from_card != "Неизвестно":
                raw_name = name_from_card

        # Возвращаем сырые данные (как сырые продукты)
        return {
            'raw_name': raw_name,  # сырое название
            'raw_price': raw_price,  # сырая цена (текст)
            'raw_url': raw_url  # сырая ссылка
        }

    # Метод для извлечения названия из ссылки
    def extract_name_from_url(self, url):
        """
        В ссылках типа /product/torsher-ralf-beige можно найти название
        """
        try:
            # Ищем в ссылке часть после /product/ и до конца или до ?
            match = re.search(r'/product/([^/?]+)', url)

            # Если нашли
            if match:
                # Берем найденную часть (например: torsher-ralf-beige)
                product_slug = match.group(1)
                # Заменяем дефисы на пробелы и делаем красивое название
                name = product_slug.replace('-', ' ').title()
                return name
        except Exception as error:
            # Если ошибка - пишем в лог, но продолжаем работать
            self.logger.warning(f"Ошибка при извлечении названия из URL: {error}")

        return "Неизвестно"

    # Метод для извлечения названия из текста карточки
    def extract_name_from_card(self, card):
        """
        Если не получилось из ссылки, ищем в тексте карточки
        """
        try:
            # Берем ВЕСЬ текст из карточки
            all_text_elements = card.css('::text')
            # Очищаем текст от пробелов и убираем пустые строки
            all_texts = [text.get().strip() for text in all_text_elements if text.get().strip()]

            # Список текстов, которые НЕ являются названиями
            excluded_texts = [
                'Купить', 'NEW', 'В наличии',
                'Размеры (ДхШхВ)', 'Размеры (ДхШхВ), см'
            ]

            # Список для потенциальных названий
            meaningful_texts = []

            # Фильтруем все тексты
            for text in all_texts:
                # Пропускаем если есть "руб" (это цена)
                if 'руб' in text.lower():
                    continue
                # Пропускаем если текст в списке исключений
                if text in excluded_texts:
                    continue
                # Пропускаем если текст содержит размеры (50x30x20)
                if re.search(r'\d+x\d+x\d+', text):
                    continue
                # Пропускаем слишком короткие тексты
                if len(text) < 10:
                    continue
                # Добавляем подходящий текст
                meaningful_texts.append(text)

            # Если нашли подходящие тексты
            if meaningful_texts:
                # Выбираем самый длинный (обычно это название)
                best_name = max(meaningful_texts, key=len)
                return best_name
            else:
                return "Неизвестно"

        except Exception as error:
            self.logger.warning(f"Ошибка при извлечении названия из карточки: {error}")
            return "Неизвестно"

    # ГЛАВНЫЙ МЕТОД ОБРАБОТКИ ДАННЫХ
    def clean_and_process_data(self, item_data):
        """
        Здесь мы превращаем сырые данные в готовые
        Выполняем 3 этапа: очистка, преобразование, фильтрация
        """

        # ============ 1. ОЧИСТКА ДАННЫХ ============

        # Очищаем название: убираем лишние пробелы
        cleaned_name = item_data['raw_name'].strip() if item_data['raw_name'] else "Неизвестно"

        # Очищаем цену: убираем пробелы
        cleaned_price = item_data['raw_price'].strip() if item_data['raw_price'] else "Цена не указана"

        # Очищаем ссылку
        cleaned_url = item_data['raw_url'] if item_data['raw_url'] else "Ссылка не найдена"

        # Дополнительная очистка цены: удаляем все кроме цифр и пробелов
        cleaned_price = re.sub(r'[^\d\s]', '', cleaned_price).strip()

        # ============ 2. ПРЕОБРАЗОВАНИЕ ДАННЫХ ============

        try:
            # Преобразуем цену в число:
            # 1. Убираем пробелы
            # 2. Проверяем что остались цифры
            # 3. Преобразуем в число
            price_number = int(cleaned_price.replace(' ', '')) if cleaned_price.replace(' ', '').isdigit() else 0
        except (ValueError, AttributeError):
            # Если ошибка - ставим 0
            price_number = 0

        # ============ 3. ФИЛЬТРАЦИЯ ДАННЫХ ============

        # Оставляем только товары дороже 1000 рублей
        if price_number < 1000:
            # Если товар дешевле 1000 - пропускаем его
            return None

        # ============ СОЗДАЕМ КРАСИВЫЙ РЕЗУЛЬТАТ ============

        processed_item = {
            'название': cleaned_name,  # очищенное название
            'цена_руб': price_number,  # цена числом
            'цена_отформатированная': f"{price_number:,} руб.".replace(',', ' '),  # красивая цена
            'ссылка': cleaned_url,  # очищенная ссылка
            'категория': 'Источники освещения'  # добавляем категорию
        }

        return processed_item

    # МЕТОД ДЛЯ СОХРАНЕНИЯ В CSV
    def save_to_csv(self):
        """
        Сохраняем все данные в CSV файл
        """

        # Если нет данных - выходим
        if not self.parsed_data:
            self.logger.info("Нет данных для сохранения")
            return

        # Имя файла для сохранения
        filename = 'data/divan_lighting_products.csv'

        try:
            # Открываем файл для записи
            with open(filename, 'w', newline='', encoding='utf-8') as file:
                # Создаем писателя CSV
                writer = csv.writer(file)

                # Пишем заголовки столбцов
                writer.writerow([
                    'Название товара',
                    'Цена (руб)',
                    'Цена отформатированная',
                    'Ссылка на товар',
                    'Категория'
                ])

                # Пишем каждую строку данных
                for item in self.parsed_data:
                    writer.writerow([
                        item['название'],  # название
                        item['цена_руб'],  # цена числом
                        item['цена_отформатированная'],  # красивая цена
                        item['ссылка'],  # ссылка
                        item['категория']  # категория
                    ])

            # Сообщаем об успехе
            self.logger.info(f"Данные успешно сохранены в файл: {filename}")
            self.logger.info(f"Всего сохранено товаров: {len(self.parsed_data)}")

        except Exception as error:
            # Сообщаем об ошибке
            self.logger.error(f"Ошибка при сохранении в CSV: {error}")

    # МЕТОД, КОТОРЫЙ ВЫЗЫВАЕТСЯ ПРИ ЗАВЕРШЕНИИ
    def closed(self, reason):
        """
        Вызывается когда паук заканчивает работу
        Гарантирует что данные сохранятся даже при ошибках
        """
        if hasattr(self, 'parsed_data') and self.parsed_data:
            self.save_to_csv()


# ФУНКЦИЯ ДЛЯ ТЕСТИРОВАНИЯ
def test_data_processing():
    """
    Можно запустить эту функцию чтобы проверить обработку данных
    без запуска всего парсера
    """
    # Тестовые данные (как с сайта)
    test_data = [
        {'raw_name': '  Торшер Ральф Beige  ', 'raw_price': '13 990руб.', 'raw_url': '/product/torsher-ralf-beige'},
        {'raw_name': 'Подвесной светильник Ферум Orange', 'raw_price': '4 990руб.',
         'raw_url': '/product/podvesnoj-svetilnik-ferum-orange'},
        {'raw_name': 'Бра Смастен White  ', 'raw_price': 'не указана', 'raw_url': '/product/bra-smasten-white'},
    ]

    # Создаем паука для тестирования
    spider = DivanLightingSpider()

    print("=" * 60)
    print("ТЕСТИРОВАНИЕ ОБРАБОТКИ ДАННЫХ")
    print("=" * 60)

    # Тестируем каждые данные
    for i, raw_item in enumerate(test_data, 1):
        # Обрабатываем данные
        processed = spider.clean_and_process_data(raw_item)

        # Если данные прошли обработку
        if processed:
            print(f"✅ {i}. ОБРАБОТАНО: {processed['название']}")
            print(f"   Цена: {processed['цена_отформатированная']}")
            print(f"   Ссылка: {processed['ссылка']}")
        else:
            print(f"❌ {i}. ОТФИЛЬТРОВАНО: {raw_item['raw_name']}")
            print(f"   Причина: цена меньше 1000 руб или не указана")

        print("-" * 40)


# Если файл запускается напрямую
if __name__ == "__main__":
    test_data_processing()