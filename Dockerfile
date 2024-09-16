FROM python:3

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r app/requirements.txt
CMD ["python", "app/telegram_bot.py"]