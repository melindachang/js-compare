import TelegramBot from 'node-telegram-bot-api';

const bot = new TelegramBot(process.env.TELEGRAM_TOKEN!, { polling: true });

const NOT_WELCOME_MESSAGE = [
  "Hi. I'm a private bot managing a count of specific Telegram channel.",
  "There is nothing I can do for you, so goodbye and have a nice day :-)\n\n",
  "Привет! Я частный бот, работающий только на парочке секретных телеграм каналов,",
  "поэтому ничем не могу вам быть полезен. До свидания и хорошего дня! :-)\n\n"
].join(" ");

function processPrivateMessage(msg: TelegramBot.Message) {
  const text = (msg.text || '').trim();

  if (text == '/ping') {
    bot.sendMessage(msg.chat.id, "Pong!");
    return;
  }

  bot.sendMessage(msg.chat.id, NOT_WELCOME_MESSAGE, { parse_mode: 'Markdown' });
}

/**********************************/

bot.on('message', msg => {
  const isPrivate = msg.chat?.type !== 'supergroup';

  if (!isPrivate) {
    return;
  }

  processPrivateMessage(msg);
});

bot.on('left_chat_members', async msg => {
  console.log(new Date().toISOString(), "left_chat_members", JSON.stringify(msg));
});

bot.on('new_chat_members', async msg => {
  console.log(new Date().toISOString(), "new_chat_members", JSON.stringify(msg));

  for (const member of msg.new_chat_members!) {
    if (member.is_bot) {
      continue;
    }

    console.log(`Deleted ${msg.new_chat_members!.length} new_chat_members`);
    bot.deleteMessage(msg.chat.id, msg.message_id);
  }
});

bot.on('polling_error', error => {
  console.log('polling_error');
  console.log(error);
});
