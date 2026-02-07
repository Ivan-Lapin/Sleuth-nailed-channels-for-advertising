from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext

from src.telegram.bot.states import Flow
from src.services.marking_service import mark_channel

router = Router()

@router.callback_query(F.data.startswith("mark:"))
async def mark_click(cb: CallbackQuery, state: FSMContext):
    # mark:potential:{brand_id}:{start_ymd}:{end_ymd}:{channel_id}
    _, status, brand_id, start_ymd, end_ymd, channel_id = cb.data.split(":")
    await state.update_data(
        mark_status=status,
        brand_id=int(brand_id),
        start_ymd=start_ymd,
        end_ymd=end_ymd,
        channel_id=int(channel_id),
        message_id=cb.message.message_id,
        chat_id=cb.message.chat.id,
    )
    await state.set_state(Flow.waiting_reason)
    await cb.message.answer("Введите причину (или '-' если без причины):")
    await cb.answer()

@router.message(Flow.waiting_reason)
async def reason_entered(message: Message, state: FSMContext, pool, settings):
    data = await state.get_data()
    status = data["mark_status"]
    brand_id = data["brand_id"]
    channel_id = data["channel_id"]

    reason = message.text.strip()
    if reason in {"-", ""}:
        reason = None

    async with pool.acquire() as conn:
        await mark_channel(conn, channel_id, brand_id, status, reason)

    await message.answer(f"Готово: статус = {status}, причина = {reason or '-'}")
    await state.clear()
