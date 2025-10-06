from flask import Flask, render_template, request, jsonify
import pandas as pd
import numpy as np
import io
import csv
import re
from datetime import datetime
import json

app = Flask(__name__)


def detect_csv_type(content):
    """Определяет тип CSV файла"""
    content_lower = content.lower()

    if 'google финанс' in content_lower or 'date,open,high,low,close,volume' in content_lower:
        return 'google_finance'
    elif 'месячный бюджет' in content_lower or 'расходы' in content_lower or 'доходы' in content_lower:
        return 'budget'
    elif any(keyword in content_lower for keyword in ['продаж', 'sales', 'revenue', 'выручка']):
        return 'sales'
    else:
        return 'generic'


def parse_complex_csv(content, file_type):
    """Универсальный парсер для сложных CSV файлов"""

    if file_type == 'google_finance':
        return parse_google_finance(content)
    elif file_type == 'budget':
        return parse_budget_file(content)
    else:
        return parse_generic_csv(content)


def parse_google_finance(content):
    """Парсер для Google Finance CSV"""
    lines = content.strip().split('\n')

    # Находим начало данных
    data_start = 0
    for i, line in enumerate(lines):
        if 'Date,Open,High,Low,Close,Volume' in line:
            data_start = i
            break

    data_lines = lines[data_start:]

    df_data = []
    reader = csv.DictReader(data_lines)

    for row in reader:
        if not row or len(row) < 6:
            continue

        try:
            # Преобразуем русские названия месяцев
            date_str = row.get('Date', '')
            month_translation = {
                'окт.': 'Oct', 'нояб.': 'Nov', 'дек.': 'Dec',
                'янв.': 'Jan', 'февр.': 'Feb', 'мар.': 'Mar',
                'апр.': 'Apr', 'мая': 'May', 'июн.': 'Jun',
                'июл.': 'Jul', 'авг.': 'Aug', 'сент.': 'Sep'
            }

            for ru, en in month_translation.items():
                date_str = date_str.replace(ru, en)

            date_str = re.sub(r'\s+', ' ', date_str.strip())
            date = datetime.strptime(date_str, '%b %d, %Y')

            # Обрабатываем числа с запятыми и пробелами
            open_val = float(row['Open'].replace(',', '.').replace(' ', ''))
            high_val = float(row['High'].replace(',', '.').replace(' ', ''))
            low_val = float(row['Low'].replace(',', '.').replace(' ', ''))
            close_val = float(row['Close'].replace(',', '.').replace(' ', ''))
            volume_val = int(row['Volume'].replace(' ', ''))

            df_data.append({
                'Date': date,
                'Open': open_val,
                'High': high_val,
                'Low': low_val,
                'Close': close_val,
                'Volume': volume_val
            })
        except Exception as e:
            continue

    return pd.DataFrame(df_data)


def parse_budget_file(content):
    """Парсер для бюджетных файлов"""
    lines = content.strip().split('\n')

    # Извлекаем финансовые данные из бюджета
    budget_data = []

    # Ищем финансовые показатели
    for i, line in enumerate(lines):
        line_clean = line.strip()

        # Извлекаем начальную сумму
        if 'начальная сумма' in line_clean.lower():
            for j in range(i, min(i + 5, len(lines))):
                amount_match = re.search(r'(\d+[\s\d]*)\s*₽', lines[j])
                if amount_match:
                    budget_data.append({
                        'Type': 'Начальная сумма',
                        'Amount': float(amount_match.group(1).replace(' ', '')),
                        'Category': 'Баланс'
                    })

        # Извлекаем расходы и доходы
        elif any(keyword in line_clean.lower() for keyword in
                 ['расходы', 'доходы', 'питание', 'здоровье', 'дом', 'транспорт']):
            cells = [cell.strip() for cell in line_clean.split(',') if cell.strip()]

            for cell in cells:
                # Ищем суммы в формате "1000 ₽"
                amount_match = re.search(r'([+-]?\d+[\s\d]*)\s*₽', cell)
                if amount_match and len(cells) > 1:
                    amount = float(amount_match.group(1).replace(' ', '').replace('+', ''))

                    category = cells[0] if cells else 'Неизвестно'
                    if any(keyword in category.lower() for keyword in ['расход', 'expense']):
                        budget_type = 'Расход'
                    elif any(keyword in category.lower() for keyword in ['доход', 'income']):
                        budget_type = 'Доход'
                    else:
                        budget_type = 'Категория'

                    budget_data.append({
                        'Type': budget_type,
                        'Category': category,
                        'Amount': amount
                    })

    # Если не нашли структурированных данных, создаем простой датафрейм
    if not budget_data:
        return pd.DataFrame({
            'Description': ['Бюджетный файл загружен', 'Используйте расширенный анализ'],
            'Value': [1, 1]
        })

    return pd.DataFrame(budget_data)


def parse_generic_csv(content):
    """Универсальный парсер для стандартных CSV"""
    try:
        # Пробуем стандартный парсинг
        df = pd.read_csv(io.StringIO(content))
        return df
    except:
        # Если не получается, пробуем парсить вручную
        lines = content.strip().split('\n')

        # Ищем строку с заголовками
        header_idx = 0
        for i, line in enumerate(lines):
            if len(line.split(',')) > 1:  # Строка с несколькими колонками
                header_idx = i
                break

        # Парсим данные
        data = []
        for line in lines[header_idx:]:
            cells = line.split(',')
            if len(cells) > 1:
                row_data = {}
                for j, cell in enumerate(cells):
                    row_data[f'Column_{j}'] = cell.strip()
                data.append(row_data)

        return pd.DataFrame(data)


def extract_metadata(content, file_type):
    """Извлекает метаданные из файла"""
    metadata = {
        'file_type': file_type,
        'description': '',
        'total_rows': 0,
        'total_columns': 0
    }

    lines = content.strip().split('\n')[:10]  # Первые 10 строк для анализа

    if file_type == 'google_finance':
        for line in lines:
            if 'Google Финанс' in line:
                metadata['description'] = 'Данные акций Google Finance'
                break
    elif file_type == 'budget':
        metadata['description'] = 'Бюджетные данные'
        for line in lines:
            if 'Месячный бюджет' in line:
                metadata['description'] = 'Месячный бюджет'
                break

    return metadata


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        file = request.files['file']
        if not file:
            return jsonify({'error': 'No file uploaded'}), 400

        # Читаем файл
        content = file.read().decode('utf-8')

        # Определяем тип файла
        file_type = detect_csv_type(content)

        # Парсим данные
        df = parse_complex_csv(content, file_type)

        if df.empty:
            return jsonify({'error': 'No valid data found in the file'}), 400

        # Извлекаем метаданные
        metadata = extract_metadata(content, file_type)
        metadata['total_rows'] = len(df)
        metadata['total_columns'] = len(df.columns)

        # Basic statistics
        stats = {}
        for column in df.columns:
            col_data = df[column]

            # Пропускаем нечисловые колонки для статистики
            if col_data.dtype in ['object', 'bool']:
                try:
                    # Пробуем преобразовать в числа
                    numeric_data = pd.to_numeric(col_data, errors='coerce')
                    if not numeric_data.isna().all():
                        col_data = numeric_data
                    else:
                        # Для текстовых колонок
                        stats[column] = {
                            'type': 'categorical',
                            'unique_count': int(col_data.nunique()),
                            'most_common': col_data.mode().iloc[0] if not col_data.mode().empty else 'N/A',
                            'count': int(col_data.count())
                        }
                        continue
                except:
                    stats[column] = {
                        'type': 'categorical',
                        'unique_count': int(col_data.nunique()),
                        'most_common': col_data.mode().iloc[0] if not col_data.mode().empty else 'N/A',
                        'count': int(col_data.count())
                    }
                    continue

            # Для числовых колонок
            if pd.api.types.is_numeric_dtype(col_data):
                stats[column] = {
                    'type': 'numeric',
                    'mean': float(col_data.mean()),
                    'median': float(col_data.median()),
                    'std': float(col_data.std()),
                    'min': float(col_data.min()),
                    'max': float(col_data.max()),
                    'count': int(col_data.count())
                }
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                stats[column] = {
                    'type': 'datetime',
                    'min_date': col_data.min().strftime('%Y-%m-%d'),
                    'max_date': col_data.max().strftime('%Y-%m-%d'),
                    'count': int(col_data.count())
                }

        # Подготовка данных для превью
        preview_df = df.head(10).copy()

        # Конвертируем даты для JSON
        for column in preview_df.columns:
            if pd.api.types.is_datetime64_any_dtype(preview_df[column]):
                preview_df[column] = preview_df[column].dt.strftime('%Y-%m-%d')

        preview_data = preview_df.to_dict('records')
        columns = list(df.columns)

        return jsonify({
            'success': True,
            'columns': columns,
            'preview': preview_data,
            'statistics': stats,
            'metadata': metadata,
            'shape': {'rows': len(df), 'cols': len(columns)},
            'data_type': file_type
        })

    except Exception as e:
        return jsonify({'error': f'Error processing file: {str(e)}'}), 500


@app.route('/sample/<sample_type>')
def get_sample_data(sample_type):
    """Возвращает sample данные для тестирования"""
    samples = {
        'google_finance': {
            'columns': ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'],
            'data': [
                {'Date': '2024-10-07', 'Open': 169.14, 'High': 169.90, 'Low': 164.13, 'Close': 164.39,
                 'Volume': 14034722},
                {'Date': '2024-10-08', 'Open': 165.43, 'High': 166.10, 'Low': 164.31, 'Close': 165.70,
                 'Volume': 11723885},
                {'Date': '2024-10-09', 'Open': 164.86, 'High': 166.26, 'Low': 161.12, 'Close': 163.06,
                 'Volume': 19666411}
            ]
        },
        'budget': {
            'columns': ['Category', 'Type', 'Amount'],
            'data': [
                {'Category': 'Питание', 'Type': 'Расход', 'Amount': 15000},
                {'Category': 'Транспорт', 'Type': 'Расход', 'Amount': 5000},
                {'Category': 'Зарплата', 'Type': 'Доход', 'Amount': 100000}
            ]
        }
    }

    if sample_type in samples:
        return jsonify(samples[sample_type])
    else:
        return jsonify({'error': 'Sample not found'}), 404


if __name__ == '__main__':
    app.run(debug=True)