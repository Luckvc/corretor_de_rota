import logging
import arrange_data
import time
from credentials import TELEGRAM_API_KEY 
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, )

async def doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_file = await update.message.effective_attachment.get_file()
    print(update.message.effective_attachment.file_name)
    print("File recieved")
    await new_file.download_to_drive('downloaded_file.xlsx')
    print("File downloaded")
    processed_df = arrange_data.process_data('downloaded_file.xlsx')
    print("Data processed")
    processed_df.to_excel('processed_data.xlsx', index=False)
    print("Data Saved")

    retry_count = 10
    attempt = 0
    success = False
    while attempt < retry_count and not success:
        try:
            await context.bot.send_document(chat_id=update.effective_chat.id, document='processed_data.xlsx')
            print("Data sent")
            success = True
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed with error: {e}")
            time.sleep(1)  # Optional: wait for a second before retrying

    if not success:
        print("Failed to execute the command after 10 attempts.")

if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_API_KEY).build()

    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)

    application.add_handler(MessageHandler(filters.ATTACHMENT, doc))

    application.run_polling()