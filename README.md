# 🗼 Paris Housing Analytics Pipeline

**End-to-end batch data pipeline + интерактивный дашборд** для анализа цен на недвижимость в Париже (20 arrondissements).

### Что внутри

- **Data Lake** → Parquet (с партиционированием по дате)
- **Data Warehouse** → SQLite + предвычисленные агрегаты
- **Оркестрация** → `orchestrate.py` (готово к Airflow)
- **Дашборд** → Streamlit + Plotly (2 обязательных тайла + дополнительные графики)

### Как запустить локально

```bash
# 1. Активировать окружение
.\venv\Scripts\Activate.ps1

# 2. Запустить пайплайн
cd pipeline
python orchestrate.py

# 3. Запустить дашборд
cd ../dashboard
streamlit run app.py
```
