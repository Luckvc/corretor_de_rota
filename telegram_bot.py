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
    message = """
        Olá, bem vindo ao corretor de rota Shopee!
    
Esse robô ainda está em fase de testes, peço paciência e qualquer problema peço que me procurem, meu nome é Lucas e estarei divulgando o robô no grupo.

O intuito desse robô é melhorar a roterização de aplicativos como o Circuit, mas notem que podem ocorrer discrepancias caso o cliente tenha colocado o CEP errado.

Para utilizar o robô é só compartilhar a planilha baixada do aplicatiov Driver Shopee. Depois abrir a planilha com o Circuit ou aplicativo de sua preferência, eu geralmente seleciono para aparecer as seguintes colunas: Qtd de Pacotes, Número dos pacotes e Complemento.

Note que pode demorar 1 ou 2 minutos para processar a planilha.

Digite /planilha caso ainda não sabia baixar a planilha do aplicativo Driver Shopee"""
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

async def planilha(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = """Para baixar a planilha é simples, abra o seu aplicativo.
        1. Vá na aba de entregas pentendes, onde aparecem todos os pedidos a serem entregues.
        2. Bem ao lado direito do botão de Mostrar no Mapa, tem o botão de baixar planilha.
        3. Com a planilha baixada, é só compartilhar o arquivo nessa conversa pelo botão de clips de papel."""
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

async def doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_file = await update.message.effective_attachment.get_file()
    raw_file_name = update.message.effective_attachment.file_name
    raw_file_path = 'data/raw/' + raw_file_name
    await new_file.download_to_drive(raw_file_path)

    processed_df = arrange_data.process_data(raw_file_path)
    processed_file_path = 'data/processed/corrigido_' + raw_file_name
    processed_df.to_excel(processed_file_path, index=False)

    retry_count = 10
    attempt = 0
    success = False
    while attempt < retry_count and not success:
        try:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=processed_file_path)
            print("Data sent")
            success = True
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed with error: {e}")
            time.sleep(1)  # Optional: wait for a second before retrying

    if not success:
        print("Failed to execute the command after 10 attempts.")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Não foi possível processar a planilha")

if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_API_KEY).build()

    application.add_handler(CommandHandler('planilha', planilha))
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT, start))


    application.add_handler(MessageHandler(filters.ATTACHMENT, doc))

    application.run_polling()