.PHONY: install run test bot stats db metrics stop clean

# 🌈 ЦВЕТА
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m

# 🏃‍♂️ УСТАНОВКА
install:
	@echo "${GREEN}📦 Устанавливаем...${NC}"
	pip install -r requirements.txt

# 🚀 ТЕСТОВЫЙ ПОИСК (только FOREO)
test:
	@echo "${GREEN}🧪 Тестовый поиск Bork...${NC}"
	make stop && rm -f data/tg_global_ads.db && python -c "import asyncio; exec(open('main.py').read()); asyncio.run(test_search())"

# 🤖 ТОЛЬКО БОТ
bot:
	@echo "${GREEN}🤖 Запуск бота...${NC}"
	make stop && SKIP_PROMETHEUS=1 python -c "import asyncio; exec(open('main.py').read()); asyncio.run(start_bot())"

# 🔥 ПОЛНЫЙ ЗАПУСК
run:
	@echo "${GREEN}🚀 Полный запуск...${NC}"
	mkdir -p data
	python main.py

# 📊 СТАТИСТИКА БД
stats:
	@echo "${GREEN}📊 Статистика БД...${NC}"
	@sqlite3 data/tg_global_ads.db "SELECT COUNT(*)||'|'||COUNT(CASE WHEN total_mentions>=5 THEN 1 END) FROM channel_stats"

# 🗄️ БАЗА ДАННЫХ
db:
	@echo "${GREEN}🗄️ Содержимое БД...${NC}"
	@sqlite3 data/tg_global_ads.db ".tables" || echo "📭 Нет БД"
	@echo ""
	@sqlite3 data/tg_global_ads.db "SELECT brand, title, username, mentioncount \
		FROM global_ads \
		WHERE mentioncount > 1 \
		ORDER BY mentioncount DESC LIMIT 10;" 2>/dev/null || echo "📭 Нет данных"

# 📈 МЕТРИКИ
metrics:
	@echo "${GREEN}📈 Prometheus метрики...${NC}"
	@curl -s localhost:8000/metrics | grep tg_ || echo "📊 Запустите make run"

# 🛑 ОСТАНОВКА
stop:
	@echo "${YELLOW}🛑 Остановка...${NC}"
	pkill -f main.py || echo "${GREEN}✅ Уже остановлено${NC}"

# 🗑️ ОЧИСТКА
clean:
	@echo "${YELLOW}🧹 Очистка...${NC}"
	rm -rf data/*.db tg_session.*
	@echo "${GREEN}✅ Очищено!${NC}"

# 📋 ПОМОЩЬ
help:
	@echo "${GREEN}🚀 TG Ad Tracker - Команды:${NC}"
	@echo "  make install     📦 Установить"
	@echo "  make test        🧪 Поиск FOREO (5 сек)"
	@echo "  make stats       📊 Статистика БД"
	@echo "  make db          🗄️ Посмотреть БД"
	@echo "  make bot         🤖 Только бот"
	@echo "  make run         🔥 Полный запуск"
	@echo "  make metrics     📈 Prometheus"
	@echo "  make stop        🛑 Остановить"
	@echo "  make clean       🧹 Полная очистка"
