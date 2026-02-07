# /detailed_stats_brand + кнопки брендов
import asyncio
from aiogram import F, Router
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from db.db import get_detailed_brand_channels
from telethon_client import get_channel_stats, get_post_stats
from aiogram.filters import Command
from config import BRANDS
from utils import format_date, safe_text, escape_markdown_v2
router = Router()

@router.message(Command("detailed_stats_brand"))
async def detailed_stats_brand_cmd(message: Message):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=brand, callback_data=f"brand_{brand}")] 
                for brand in BRANDS
            ]
    )
    await message.answer("🔍 Выберите бренд для подробной статистики:", reply_markup=keyboard)   

@router.callback_query(F.data.startswith('brand_'))
async def brand_channels_callback(callback: CallbackQuery):
        brand = callback.data.split('_', 1)[1]
        
        channels = get_detailed_brand_channels(brand, min_mentions=2)
        
        tasks = []
        for i, (ch_name, username, mentions, posts_data) in enumerate(channels[:5]):
            if username and posts_data:
                post_ids = [post[2] for post in posts_data[:3]]
                tasks.append((i, username, post_ids))
                
        channel_results = {}
        post_results = {}
                
        if tasks:
            
            channel_tasks = [get_channel_stats(username) for i, username, post_ids in tasks]
            channel_results_list = await asyncio.gather(*channel_tasks, return_exceptions=True)
   
            post_tasks = []
            for i, username, post_ids in tasks:
                for post_id in post_ids:
                    post_tasks.append(get_post_stats(username, post_id))
            post_results_list = await asyncio.gather(*post_tasks, return_exceptions=True)
            
        ch_idx = 0
        post_idx = 0
        for task_idx, (i, username, post_ids) in enumerate(tasks):
            channel_results[i] = channel_results_list[ch_idx] if ch_idx < len(channel_results_list) else {}
            ch_idx += 1
            
            channel_posts = {}
            for _ in post_ids:
                post_result = post_results_list[post_idx] if post_idx < len(post_results_list) else {}
                if isinstance(post_result, dict) and 'error' not in post_result:
                    post_id_str = str(list(post_result.keys())[0]) if post_result else '0'
                    channel_posts[post_id_str] = post_result.get(post_id_str, {})
                post_idx += 1
            post_results[i] = channel_posts
        
    
        text = f"🔍 Каналы для бренда {brand}\n\n"
    
        for i, (ch_name, username, mentions, posts_data) in enumerate(channels[:8], 1):
            safe_username = username or 'private'
            safe_ch_name = (ch_name or 'Без названия')[:25]
            
            # Подписчики из API или 0
            ch_stats = channel_results.get(i-1, {})
            subscribers = ch_stats.get('subscribers', 0) if isinstance(ch_stats, dict) else 0
            
            text += f"{i}. @{safe_username} — {safe_ch_name} ({subscribers:,}) x{mentions}\n"
            
            # Посты с метриками
            if posts_data:
                for j, (post_id_str, post_date_str, msg_id) in enumerate(posts_data[:3], 1):
                    post_stats = post_results.get(i-1, {}).get(str(msg_id), {})
                    views = post_stats.get('views', 0)
                    forwards = post_stats.get('forwards', 0)
                    
                    post_link = f"https://t.me/{safe_username}/{msg_id}"
                    date_formatted = post_date_str[:10].replace('-', '.')
                    
                    text += f"   {j}. {date_formatted} | 👁{views:,} 🔄{forwards} | [{post_link}]({post_link})\n"
                text += "\n"
        
        if not channels:
            text += "❌ Каналов не найдено"
        
        await callback.message.edit_text(
            text, 
            parse_mode=None 
        )
        await callback.answer()