AeroVPN Telegram Bot

Что внутри:
- main.py — весь бот
- requirements.txt — зависимости

Как запустить локально:
1. Python 3.11+
2. pip install -r requirements.txt
3. python main.py

Что уже встроено:
- токен бота
- ссылки AeroVPN
- Platega MerchantId и Secret
- цена 100 ₽
- SQLite база
- проверка подписки на канал
- оплата СБП и криптой
- рефералка
- админка

Важно:
1. Добавьте бота в канал @AeroVPNpro и дайте ему право видеть участников.
2. Если хотите доступ к /admin, в main.py впишите свой Telegram ID в ADMIN_IDS_RAW через переменную окружения ADMIN_IDS
   или замените пустое значение на ваш ID прямо в коде.
3. На хостинге нужен обычный запуск команды: python main.py

Для GitHub:
Загружайте файлы в корень репозитория.
