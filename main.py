import asyncio
import logging
import os
from html import escape as esc

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, ChatMemberUpdatedFilter, ADMINISTRATOR, IS_NOT_MEMBER, MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    ChatMemberUpdated,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Ձեր Telegram user id (ադմինի համար)
MONGODB_URI = os.getenv("MONGODB_URI")  # MongoDB Atlas connection string
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "ban_bot")
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "6"))  # պարբերական ինքնաստուգում

logging.basicConfig(level=logging.INFO)
router = Router()

# ---------------------------------------------------------------------------
# Database — MongoDB Atlas (motor՝ async driver)։ Տվյալները պահվում են
# Railway-ից ամբողջովին անկախ, ուստի Redeploy-ները/Update-ները ոչինչ չեն ջնջում
# ---------------------------------------------------------------------------

_mongo_client: AsyncIOMotorClient | None = None
_channels = None  # MongoDB collection
_bot_id: int | None = None


async def init_db():
    global _mongo_client, _channels
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI չի գտնվել: Railway-ում պետք է սահմանել այս փոփոխականը!")
    _mongo_client = AsyncIOMotorClient(MONGODB_URI)
    db = _mongo_client[MONGODB_DB_NAME]
    _channels = db["channels"]
    await _channels.create_index("chat_id", unique=True)
    await _channels.create_index("owner_id")


async def close_db():
    if _mongo_client:
        _mongo_client.close()


async def upsert_channel(chat_id: int, chat_title: str, owner_id: int, can_ban: bool):
    await _channels.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "chat_title": chat_title,
                "owner_id": owner_id,
                "active": True,
                "can_ban": can_ban,
            },
            "$setOnInsert": {"enabled": True, "banned_count": 0},
        },
        upsert=True,
    )


async def set_channel_status(chat_id: int, active: bool, can_ban: bool | None = None):
    update = {"active": active}
    if can_ban is not None:
        update["can_ban"] = can_ban
    await _channels.update_one({"chat_id": chat_id}, {"$set": update})


async def get_channel(chat_id: int):
    return await _channels.find_one({"chat_id": chat_id})


async def get_user_channels(owner_id: int):
    cursor = _channels.find({"owner_id": owner_id, "active": True})
    return await cursor.to_list(length=200)


async def toggle_channel(chat_id: int) -> bool:
    channel = await _channels.find_one({"chat_id": chat_id})
    new_val = not channel.get("enabled", True)
    await _channels.update_one({"chat_id": chat_id}, {"$set": {"enabled": new_val}})
    return new_val


async def increment_ban_count(chat_id: int):
    await _channels.update_one({"chat_id": chat_id}, {"$inc": {"banned_count": 1}})


async def count_active_channels() -> int:
    return await _channels.count_documents({"active": True, "can_ban": True})


async def get_all_stored_channel_ids():
    cursor = _channels.find({}, {"chat_id": 1})
    return [doc["chat_id"] async for doc in cursor]


async def get_all_owner_ids():
    """Ալիք-սեփականատերերի եզակի ID-ներ, որոնց ալիքում բոտն ընթացիկ ադմին է
    (օգտագործվում է «Ուղարկել բոլորին» ֆունկցիայի համար)։"""
    return await _channels.distinct("owner_id", {"active": True})


# ---------------------------------------------------------------------------
# Telegram API օգնականներ (cache + rate-limit-ի հանդեպ դիմացկուն կանչեր)
# ---------------------------------------------------------------------------

async def get_bot_id(bot: Bot) -> int:
    global _bot_id
    if _bot_id is None:
        me = await bot.get_me()
        _bot_id = me.id
    return _bot_id


async def safe_ban(bot: Bot, chat_id: int, user_id: int, retries: int = 3) -> bool:
    """Բանում է օգտատիրոջը, ինքնուրույն սպասելով, եթե Telegram-ը ժամանակավորապես
    սահմանափակել է հարցումների արագությունը (flood control)։"""
    for attempt in range(retries):
        try:
            await bot.ban_chat_member(chat_id, user_id)
            return True
        except TelegramRetryAfter as e:
            logging.warning(f"Flood control. սպասում ենք {e.retry_after}վ (փորձ {attempt + 1})")
            await asyncio.sleep(e.retry_after)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logging.warning(f"Չհաջողվեց արգելափակել {user_id} ալիքում {chat_id}. {e}")
            return False
    return False


async def safe_send(bot: Bot, user_id: int, text: str, retries: int = 2) -> bool:
    """Ուղարկում է հաղորդագրություն flood-control-ի հանդեպ դիմացկուն կերպով,
    օգտագործվում է broadcast-ի ժամանակ։"""
    for attempt in range(retries):
        try:
            await bot.send_message(user_id, text)
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except (TelegramBadRequest, TelegramForbiddenError):
            return False
    return False


# ---------------------------------------------------------------------------
# Իրական ստուգում Telegram API-ի միջոցով (ոչ միայն eventներով)
# ---------------------------------------------------------------------------

async def verify_channel(bot: Bot, chat_id: int) -> bool:
    try:
        bot_id = await get_bot_id(bot)
        member = await bot.get_chat_member(chat_id, bot_id)
        is_admin = member.status == "administrator"
        can_ban = bool(getattr(member, "can_restrict_members", False)) if is_admin else False
        await set_channel_status(chat_id, active=is_admin, can_ban=can_ban)
        return is_admin and can_ban
    except Exception:
        await set_channel_status(chat_id, active=False, can_ban=False)
        return False


async def verify_all_channels(bot: Bot) -> int:
    """Ստուգում է ԲՈԼՈՐ պահված ալիքները և վերադարձնում փաստացի ակտիվների քանակը։"""
    chat_ids = await get_all_stored_channel_ids()
    active = 0
    for chat_id in chat_ids:
        if await verify_channel(bot, chat_id):
            active += 1
    return active


async def periodic_channel_check(bot: Bot):
    """Ֆոնային ցիկլ, որը ամեն CHECK_INTERVAL_HOURS ժամը մեկ ինքնուրույն ստուգում է
    բոլոր ալիքները, որպեսզի բացթողած update-երից հետո թիվը միշտ ճշգրիտ մնա։"""
    while True:
        await asyncio.sleep(CHECK_INTERVAL_HOURS * 3600)
        try:
            count = await verify_all_channels(bot)
            logging.info(f"Պարբերական ստուգում ավարտվեց. ֆունկցիոնալ ալիքներ՝ {count}")
        except Exception as e:
            logging.warning(f"Պարբերական ստուգման սխալ. {e}")


# ---------------------------------------------------------------------------
# Բոտին ադմին կարգավիճակ տալը / հեռացնելը / իջեցնելը
# ---------------------------------------------------------------------------

@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=ADMINISTRATOR))
async def on_bot_promoted(event: ChatMemberUpdated):
    can_ban = bool(getattr(event.new_chat_member, "can_restrict_members", False))
    await upsert_channel(
        event.chat.id, event.chat.title or str(event.chat.id), event.from_user.id, can_ban
    )
    title = esc(event.chat.title or str(event.chat.id))
    try:
        if can_ban:
            await event.bot.send_message(
                event.from_user.id,
                f"✅ Բոտը հաջողությամբ ավելացվեց որպես ադմին «{title}» ալիքում։\n\n"
                f"<blockquote>Ավտոմատ արգելափակումն այժմ ակտիվ է։ "
                f"Կարգավորումների համար՝ /start</blockquote>",
            )
        else:
            await event.bot.send_message(
                event.from_user.id,
                f"⚠️ Բոտն ավելացվեց «{title}» ալիքում, բայց ՉՈՒՆԻ "
                f"«Անդամներին սահմանափակել» իրավունքը։\n\n"
                f"<blockquote>Դեռ ՉԻ կարող ոչ ոքի արգելափակել։ Խնդրում ենք ադմինի "
                f"կարգավորումներում միացնել այս իրավունքը։</blockquote>",
            )
    except Exception:
        pass


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER))
async def on_bot_removed(event: ChatMemberUpdated):
    await set_channel_status(event.chat.id, active=False, can_ban=False)


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def on_bot_demoted(event: ChatMemberUpdated):
    await set_channel_status(event.chat.id, active=False, can_ban=False)


# ---------------------------------------------------------------------------
# Օգտատերը փաստացի լքում է ալիքը → արգելափակում
# ---------------------------------------------------------------------------

REAL_MEMBER_STATUSES = {"member", "administrator", "creator"}
LEFT_STATUSES = {"left", "kicked"}


@router.chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    if old_status not in REAL_MEMBER_STATUSES or new_status not in LEFT_STATUSES:
        return

    channel = await get_channel(event.chat.id)
    if not channel or not channel.get("enabled") or not channel.get("can_ban"):
        return

    user = event.old_chat_member.user
    if user.is_bot:
        return

    try:
        current = await event.bot.get_chat_member(event.chat.id, user.id)
        if current.status == "kicked":
            return
    except Exception:
        pass

    if await safe_ban(event.bot, event.chat.id, user.id):
        await increment_ban_count(event.chat.id)
        logging.info(f"Արգելափակվեց {user.id} ալիքում {event.chat.id}")


# ---------------------------------------------------------------------------
# Broadcast (միայն OWNER_ID-ի համար) — հաղորդագրություն բոլոր ալիք-սեփականատերերին
# ---------------------------------------------------------------------------

class BroadcastStates(StatesGroup):
    waiting_for_text = State()
    confirm = State()


@router.callback_query(F.data == "broadcast_start")
async def cb_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Դու իրավունք չունես տեսնելու սա։", show_alert=True)
        return
    await state.set_state(BroadcastStates.waiting_for_text)
    await callback.message.edit_text(
        "✍️ Գրիր տեքստը, որը կուղարկվի բոլոր ալիք-սեփականատերերին, ովքեր բոտն "
        "ունեն որպես ադմին։\n\n"
        "<blockquote>Չեղարկելու համար՝ /cancel</blockquote>",
    )
    await callback.answer()


@router.message(BroadcastStates.waiting_for_text, Command("cancel"))
async def cmd_cancel_broadcast(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Չեղարկվեց։", reply_markup=main_menu_kb(message.from_user.id))


@router.message(BroadcastStates.waiting_for_text)
async def process_broadcast_text(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    text = message.html_text or esc(message.text or "")
    await state.update_data(broadcast_text=text)
    await state.set_state(BroadcastStates.confirm)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Ուղարկել", callback_data="broadcast_send")],
            [InlineKeyboardButton(text="❌ Չեղարկել", callback_data="broadcast_cancel")],
        ]
    )
    await message.answer(
        f"<b>Նախադիտում</b>\n\n<blockquote>{text}</blockquote>\n\n"
        f"Ուղարկե՞լ սա բոլոր ալիք-սեփականատերերին։",
        reply_markup=kb,
    )


@router.callback_query(F.data == "broadcast_cancel")
async def cb_broadcast_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "❌ Չեղարկվեց։", reply_markup=main_menu_kb(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "broadcast_send")
async def cb_broadcast_send(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Դու իրավունք չունես տեսնելու սա։", show_alert=True)
        return

    data = await state.get_data()
    text = data.get("broadcast_text", "")
    await state.clear()

    if not text:
        await callback.answer("Տեքստը դատարկ է։", show_alert=True)
        return

    await callback.message.edit_text("⏳ Ուղարկվում է...")

    owner_ids = await get_all_owner_ids()
    sent, failed = 0, 0
    for uid in owner_ids:
        if await safe_send(callback.bot, uid, text):
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.05)  # flood-control-ից խուսափելու համար

    result_text = f"✅ Ուղարկվեց {sent} օգտատիրոջ։"
    if failed:
        result_text += f"\n⚠️ {failed} օգտատիրոջ չհասավ (հավանաբար արգելափակել են բոտը)։"

    await callback.message.edit_text(result_text, reply_markup=main_menu_kb(callback.from_user.id))
    await callback.answer()


# ---------------------------------------------------------------------------
# Մասնավոր մենյու
# ---------------------------------------------------------------------------

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="📋 Իմ ալիքները", callback_data="my_channels")]]
    if user_id == OWNER_ID:
        rows.append(
            [InlineKeyboardButton(text="📊 Ընդհանուր վիճակագրություն", callback_data="admin_stats")]
        )
        rows.append(
            [InlineKeyboardButton(text="📨 Ուղարկել բոլորին", callback_data="broadcast_start")]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    text = (
        "👋 <b>Բարի գալուստ</b>\n\n"
        "<blockquote>🛡 Այս բոտն ավտոմատ կերպով արգելափակում է այն օգտատերերին, "
        "ովքեր լքում են քո ալիքը (եթե բոտն ավելացված է որպես ադմին)։</blockquote>\n\n"
        "📋 «Իմ ալիքները» — կառավարիր քո ալիքները (միացնել/անջատել, տես քո ալիքի "
        "արգելափակումների քանակը)։\n"
    )
    if message.from_user.id == OWNER_ID:
        text += (
            "📊 «Ընդհանուր վիճակագրություն» — միայն դու ես տեսնում, "
            "թե ընդհանուր քանի ալիք է ներկայումս օգտագործում բոտը (որպես ադմին)։\n"
            "📨 «Ուղարկել բոլորին» — ուղարկիր հաղորդագրություն բոլոր ալիք-սեփականատերերին։\n"
        )
    text += "\nԸնտրիր ցանկից՝"
    await message.answer(text, reply_markup=main_menu_kb(message.from_user.id))


@router.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "Ընտրիր ցանկից՝", reply_markup=main_menu_kb(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "my_channels")
async def cb_my_channels(callback: CallbackQuery):
    channels = await get_user_channels(callback.from_user.id)
    if not channels:
        await callback.message.edit_text(
            "<blockquote>Դու դեռ ոչ մի ալիքում չես ավելացրել բոտը որպես ադմին։</blockquote>",
            reply_markup=main_menu_kb(callback.from_user.id),
        )
        await callback.answer()
        return

    rows = [
        [InlineKeyboardButton(text=ch["chat_title"], callback_data=f"chan_{ch['chat_id']}")]
        for ch in channels
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Հետ", callback_data="back_main")])
    await callback.message.edit_text(
        "Ընտրիր ալիքը՝", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


async def render_channel_menu(callback: CallbackQuery, chat_id: int):
    channel = await get_channel(chat_id)
    if not channel:
        await callback.answer("Ալիքը չի գտնվել։", show_alert=True)
        return

    status = "🟢 Ակտիվ" if channel.get("enabled") else "🔴 Անջատված"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Փոխարկել ({status})", callback_data=f"toggle_{chat_id}")],
            [InlineKeyboardButton(text="📊 Այս ալիքի վիճակագրություն", callback_data=f"stats_{chat_id}")],
            [InlineKeyboardButton(text="⬅️ Հետ", callback_data="my_channels")],
        ]
    )
    await callback.message.edit_text(
        f"Ալիք՝ {esc(channel['chat_title'])}\n"
        f"Կարգավիճակ՝ {status}\n\n"
        f"<blockquote>ℹ️ «Փոխարկել» միացնում/անջատում է ավտոմատ արգելափակումը միայն "
        f"այս ալիքի համար։\nℹ️ «Վիճակագրություն»-ը ցույց է տալիս, թե քանի մարդ արդեն "
        f"արգելափակվել է հենց այս ալիքից։</blockquote>",
        reply_markup=kb,
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
        f"Այս ալիքում արգելափակված է {channel.get('banned_count', 0)} օգտատեր։", show_alert=True
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Դու իրավունք չունես տեսնելու սա։", show_alert=True)
        return
    count = await count_active_channels()
    await callback.answer(f"🤖 Բոտը ներկայումս ադմին է {count} ալիքում։", show_alert=True)


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN չի գտնվել .env ֆայլում!")

    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)

    try:
        initial_count = await verify_all_channels(bot)
        logging.info(f"Մեկնարկային ստուգում ավարտվեց. ֆունկցիոնալ ալիքներ՝ {initial_count}")

        asyncio.create_task(periodic_channel_check(bot))

        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(
            bot, allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
        )
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
    
