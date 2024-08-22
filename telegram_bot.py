import logging
import arrange_data
import time
import polars as pl
from credentials import TELEGRAM_API_KEY 
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters
import logging.config

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'formatter': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console_log': {
            'class': 'logging.StreamHandler',
            'formatter': 'formatter',
            'level': 'INFO',
        },
        'log_file': {
            'class': 'logging.FileHandler',
            'filename': 'log_file.log',
            'mode': 'a',
            'formatter': 'formatter',
            'level': 'WARNING',
        },
    },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['console_log', 'log_file'],
        },
    },
})


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = """
        Olá, bem vindo ao corretor de rota Shopee!
    
Esse robô ainda está em fase de testes, pedimos paciência e qualquer problema entre em contato no /help.

O intuito desse robô é melhorar a roterização de aplicativos como o Circuit, mas notem que podem ocorrer discrepâncias caso o cliente tenha colocado o CEP errado.

Para utilizar o robô é só compartilhar a planilha baixada do aplicativo Driver Shopee. Depois abrir a planilha com o Circuit ou aplicativo de sua preferência, eu geralmente seleciono para aparecer as seguintes colunas: Qtd de Pacotes, Número dos pacotes e Complemento.

Note que pode demorar 1 ou 2 minutos para processar a planilha.

Digite /planilha caso ainda não sabia baixar a planilha do aplicativo Driver Shopee"""
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

async def planilha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = """Para baixar a planilha siga os seguintes passos, abra o seu aplicativo.
        1. Vá na aba de entregas pentendes, onde aparece todos os pedidos a serem entregues.
        2. Bem ao lado direito do botão de Mostrar no Mapa, tem o botão de baixar planilha.
        3. Com a planilha baixada, é só compartilhar o arquivo nessa conversa pelo botão de clips de papel."""
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = """Qualquer problema ou sugestão, favor entrar em contato via whatsapp: (43) 98815-6626"""
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

async def doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_file = await update.message.effective_attachment.get_file()
    raw_file_name = update.message.effective_attachment.file_name
    logging.log(logging.WARNING, "arquivo " + raw_file_name + " recebido de chat_id: " + str(update.effective_chat.id))
    raw_file_path = 'data/original/' + raw_file_name
    await new_file.download_to_drive(raw_file_path)

    try:
        processed_df = arrange_data.process_data(raw_file_path)
    except Exception as e: 
        logging.log(logging.ERROR, "arquivo " + raw_file_name + " não foi processado, erro: " + str(e))
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Não foi possível processar a planilha, verifique se enviou o arquivo correto e tente novamente. Caso persistir entre em contato no /help")
    
    processed_file_path = 'data/processed/corrigido_' + raw_file_name
    processed_df.write_excel(processed_file_path, autofit=True)

    retry_count = 30
    attempt = 0
    success = False
    last_error = ''
    while attempt < retry_count and not success:
        try:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=processed_file_path)
            logging.log(logging.WARNING, "arquivo " + raw_file_name + " enviado")
            success = True
        except Exception as e:
            last_error = e
            attempt += 1
            print(f"Attempt {attempt} failed with error: {e}")
            time.sleep(0.25)

    if not success:
        logging.log(logging.ERROR, "arquivo " + raw_file_name + " não foi enviado, erro: " + str(last_error))
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Não foi possível enviar a planilha processada, tente novamente. Caso persistir entre em contato no /help")

if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_API_KEY).build()

    application.add_handler(CommandHandler('planilha', planilha))
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help))
    application.add_handler(MessageHandler(filters.TEXT, start))


    application.add_handler(MessageHandler(filters.ATTACHMENT, doc))

    application.run_polling()