import asyncio
import logging
import os

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, ChatMemberUpdatedFilter, ADMINISTRATOR, IS_NOT_MEMBER
from aiogram.types import (
    Message,
    ChatMemberUpdated,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Ձեր Telegram user id (ադմինի համար)
DB_PATH = "bot.db"

logging.basicConfig(level=logging.INFO)
router = Router()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT,
                owner_id INTEGER,
                enabled INTEGER DEFAULT 1,
                banned_count INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            )
            """
        )
        await db.commit()


async def upsert_channel(chat_id: int, chat_title: str, owner_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO channels (chat_id, chat_title, owner_id, enabled, active)
            VALUES (?, ?, ?, 1, 1)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                owner_id = excluded.owner_id,
                active = 1
            """,
            (chat_id, chat_title, owner_id),
        )
        await db.commit()


async def set_channel_inactive(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE channels SET active = 0 WHERE chat_id = ?", (chat_id,))
        await db.commit()


async def get_channel(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM channels WHERE chat_id = ?", (chat_id,))
        return await cur.fetchone()


async def get_user_channels(owner_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM channels WHERE owner_id = ? AND active = 1", (owner_id,)
        )
        return await cur.fetchall()


async def toggle_channel(chat_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT enabled FROM channels WHERE chat_id = ?", (chat_id,))
        row = await cur.fetchone()
        new_val = 0 if row["enabled"] else 1
        await db.execute("UPDATE channels SET enabled = ? WHERE chat_id = ?", (new_val, chat_id))
        await db.commit()
        return new_val


async def increment_ban_count(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE channels SET banned_count = banned_count + 1 WHERE chat_id = ?", (chat_id,)
        )
        await db.commit()


async def count_active_channels() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM channels WHERE active = 1")
        row = await cur.fetchone()
        return row[0]


# ---------------------------------------------------------------------------
# Բոտին ադմին կարգավիճակ տալը / հեռացնելը
# ---------------------------------------------------------------------------

@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=ADMINISTRATOR))
async def on_bot_promoted(event: ChatMemberUpdated):
    await upsert_channel(event.chat.id, event.chat.title or str(event.chat.id), event.from_user.id)
    try:
        await event.bot.send_message(
            event.from_user.id,
            f"✅ Բոտը հաջողությամբ ավելացվեց որպես ադմին «{event.chat.title}» ալիքում։\n"
            f"Ավտոմատ արգելափակումն այժմ ակտիվ է։ Կարգավորումների համար՝ /start",
        )
    except Exception:
        pass


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER))
async def on_bot_removed(event: ChatMemberUpdated):
    await set_channel_inactive(event.chat.id)


# ---------------------------------------------------------------------------
# Օգտատերը լքում է ալիքը → արգելափակում
# ---------------------------------------------------------------------------

@router.chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER))
async def on_member_left(event: ChatMemberUpdated):
    channel = await get_channel(event.chat.id)
    if not channel or not channel["enabled"]:
        return

    user = event.old_chat_member.user
    if user.is_bot:
        return

    try:
        await event.bot.ban_chat_member(event.chat.id, user.id)
        await increment_ban_count(event.chat.id)
        logging.info(f"Արգելափակվեց {user.id} ալիքում {event.chat.id}")
    except Exception as e:
        logging.warning(f"Չհաջողվեց արգելափակել {user.id}: {e}")


# ---------------------------------------------------------------------------
# Մասնավոր մենյու
# ---------------------------------------------------------------------------

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="📋 Իմ ալիքները", callback_data="my_channels")]]
    if user_id == OWNER_ID:
        rows.append(
            [InlineKeyboardButton(text="📊 Ընդհանուր վիճակագրություն", callback_data="admin_stats")]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Բարի գալուստ։\n\n"
        "Այս բոտն ավտոմատ կերպով արգելափակում է այն օգտատերերին, ովքեր լքում են քո ալիքը "
        "(եթե բոտն ավելացված է որպես ադմին)։\n\n"
        "Ընտրիր ցանկից՝",
        reply_markup=main_menu_kb(message.from_user.id),
    )


@router.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery):
    await callback.message.edit_text(
        "Ընտրիր ցանկից՝", reply_markup=main_menu_kb(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "my_channels")
async def cb_my_channels(callback: CallbackQuery):
    channels = await get_user_channels(callback.from_user.id)
    if not channels:
        await callback.message.edit_text(
            "Դու դեռ ոչ մի ալիքում չես ավելացրել բոտը որպես ադմին։",
            reply_markup=main_menu_kb(callback.from_user.id),
        )
        await callback.answer()
        return

    rows = [
        [InlineKeyboardButton(text=ch["chat_title"], callback_data=f"chan_{ch['chat_id']}")]
        for ch in channels
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Հետ", callback_data="back_main")])
    await callback.message.edit_text("Ընտրիր ալիքը՝", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()


async def render_channel_menu(callback: CallbackQuery, chat_id: int):
    channel = await get_channel(chat_id)
    if not channel:
        await callback.answer("Ալիքը չի գտնվել։", show_alert=True)
        return

    status = "🟢 Ակտիվ" if channel["enabled"] else "🔴 Անջատված"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Փոխարկել ({status})", callback_data=f"toggle_{chat_id}")],
            [InlineKeyboardButton(text="📊 Վիճակագրություն", callback_data=f"stats_{chat_id}")],
            [InlineKeyboardButton(text="⬅️ Հետ", callback_data="my_channels")],
        ]
    )
    await callback.message.edit_text(
        f"Ալիք՝ {channel['chat_title']}\nԿարգավիճակ՝ {status}", reply_markup=kb
    )


@router.callback_query(F.data.startswith("chan_"))
async def cb_channel_menu(callback: CallbackQuery):
    chat_id = int(callback.data.split("_", 1)[1])
    await render_channel_menu(callback, chat_id)
    await callback.answer()


@router.callback_query(F.data.startswith("toggle_"))
async def cb_toggle(callback: CallbackQuery):
    chat_id = int(callback.data.split("_", 1)[1])
    await toggle_channel(chat_id)
    await render_channel_menu(callback, chat_id)
    await callback.answer("Փոփոխված է ✅")


@router.callback_query(F.data.startswith("stats_"))
async def cb_stats(callback: CallbackQuery):
    chat_id = int(callback.data.split("_", 1)[1])
    channel = await get_channel(chat_id)
    await callback.answer(
        f"Այս ալիքում արգելափակված է {channel['banned_count']} օգտատեր։", show_alert=True
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Դու իրավունք չունես տեսնելու սա։", show_alert=True)
        return
    count = await count_active_channels()
    await callback.answer(f"Բոտն ընդհանուր օգտագործվում է {count} ալիքում։", show_alert=True)


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN չի գտնվել .env ֆայլում!")

    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(
        bot, allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
    )


if __name__ == "__main__":
    asyncio.run(main())
