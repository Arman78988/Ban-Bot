import asyncio
import logging
import os

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, ChatMemberUpdatedFilter, ADMINISTRATOR, IS_NOT_MEMBER, MEMBER
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
DB_PATH = os.getenv("DB_PATH", "bot.db")  # Railway-ում՝ /data/bot.db (եթե Volume կա)
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "6"))  # պարբերական ինքնաստուգում

logging.basicConfig(level=logging.INFO)
router = Router()

# ---------------------------------------------------------------------------
# Database — մեկ ընդհանուր կապ (ոչ թե յուրաքանչյուր հարցման համար նոր կապ),
# որպեսզի Railway-ի volume-ի հետ աշխատանքը արագ ու կայուն լինի, և
# asyncio.Lock-ը երաշխավորում է, որ գրառումները չեն բախվում միմյանց հետ
# ---------------------------------------------------------------------------

_db: aiosqlite.Connection | None = None
_db_lock = asyncio.Lock()
_bot_id: int | None = None


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row
    # WAL = ավելի արագ ու կայուն համաժամանակյա կարդալ/գրել աշխատանք
    await _db.execute("PRAGMA journal_mode=WAL;")
    await _db.execute("PRAGMA busy_timeout=5000;")
    await _db.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            owner_id INTEGER,
            enabled INTEGER DEFAULT 1,
            banned_count INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            can_ban INTEGER DEFAULT 0
        )
        """
    )
    try:
        await _db.execute("ALTER TABLE channels ADD COLUMN can_ban INTEGER DEFAULT 0")
    except Exception:
        pass  # սյունակն արդեն գոյություն ունի (անվտանգ migration update-ների ժամանակ)
    await _db.commit()


async def close_db():
    if _db:
        await _db.close()


async def upsert_channel(chat_id: int, chat_title: str, owner_id: int, can_ban: bool):
    async with _db_lock:
        await _db.execute(
            """
            INSERT INTO channels (chat_id, chat_title, owner_id, enabled, active, can_ban)
            VALUES (?, ?, ?, 1, 1, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                owner_id = excluded.owner_id,
                active = 1,
                can_ban = excluded.can_ban
            """,
            (chat_id, chat_title, owner_id, 1 if can_ban else 0),
        )
        await _db.commit()


async def set_channel_status(chat_id: int, active: bool, can_ban: bool | None = None):
    async with _db_lock:
        if can_ban is None:
            await _db.execute(
                "UPDATE channels SET active = ? WHERE chat_id = ?", (1 if active else 0, chat_id)
            )
        else:
            await _db.execute(
                "UPDATE channels SET active = ?, can_ban = ? WHERE chat_id = ?",
                (1 if active else 0, 1 if can_ban else 0, chat_id),
            )
        await _db.commit()


async def get_channel(chat_id: int):
    async with _db_lock:
        cur = await _db.execute("SELECT * FROM channels WHERE chat_id = ?", (chat_id,))
        return await cur.fetchone()


async def get_user_channels(owner_id: int):
    async with _db_lock:
        cur = await _db.execute(
            "SELECT * FROM channels WHERE owner_id = ? AND active = 1", (owner_id,)
        )
        return await cur.fetchall()


async def toggle_channel(chat_id: int) -> int:
    async with _db_lock:
        cur = await _db.execute("SELECT enabled FROM channels WHERE chat_id = ?", (chat_id,))
        row = await cur.fetchone()
        new_val = 0 if row["enabled"] else 1
        await _db.execute("UPDATE channels SET enabled = ? WHERE chat_id = ?", (new_val, chat_id))
        await _db.commit()
        return new_val


async def increment_ban_count(chat_id: int):
    async with _db_lock:
        await _db.execute(
            "UPDATE channels SET banned_count = banned_count + 1 WHERE chat_id = ?", (chat_id,)
        )
        await _db.commit()


async def count_active_channels() -> int:
    async with _db_lock:
        cur = await _db.execute("SELECT COUNT(*) FROM channels WHERE active = 1 AND can_ban = 1")
        row = await cur.fetchone()
        return row[0]


async def get_all_stored_channel_ids():
    async with _db_lock:
        cur = await _db.execute("SELECT chat_id FROM channels")
        return [row[0] for row in await cur.fetchall()]


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
    սահմանափակել է հարցումների արագությունը (flood control) — սա կանխում է
    բոտի «կախվածությունը» ծանրաբեռնվածության ժամանակ։"""
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


# ---------------------------------------------------------------------------
# Իրական ստուգում Telegram API-ի միջոցով (ոչ միայն eventներով)
# ---------------------------------------------------------------------------
# Բազան ուղղորդվում է 2 եղանակով.
#   1) Իրադարձություններով (my_chat_member) — իրական ժամանակում, երբ բոտին
#      ադմին են դարձնում կամ հեռացնում
#   2) Պարբերական ինքնաստուգումով (ստորև) — ստուգում է Telegram API-ից
#      փաստացի կարգավիճակը, որպեսզի եթե բոտն անջատված է եղել update-ի պահին
#      (redeploy, rebuild, պարբերաբար պատահող failure), տվյալները ինքնուրույն
#      ուղղվեն, ոչ թե ընդմիշտ սխալ մնան
async def verify_channel(bot: Bot, chat_id: int) -> bool:
    try:
        bot_id = await get_bot_id(bot)
        member = await bot.get_chat_member(chat_id, bot_id)
        is_admin = member.status == "administrator"
        can_ban = bool(getattr(member, "can_restrict_members", False)) if is_admin else False
        await set_channel_status(chat_id, active=is_admin, can_ban=can_ban)
        return is_admin and can_ban
    except Exception:
        # Ալիքը հասանելի չէ (բոտը հեռացված է, արգելափակված, ալիքը ջնջված է և այլն)
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
    try:
        if can_ban:
            await event.bot.send_message(
                event.from_user.id,
                f"✅ Բոտը հաջողությամբ ավելացվեց որպես ադմին «{event.chat.title}» ալիքում։\n"
                f"Ավտոմատ արգելափակումն այժմ ակտիվ է։ Կարգավորումների համար՝ /start",
            )
        else:
            await event.bot.send_message(
                event.from_user.id,
                f"⚠️ Բոտն ավելացվեց «{event.chat.title}» ալիքում, բայց ՉՈՒՆԻ "
                f"«Անդամներին սահմանափակել» իրավունքը, ուստի դեռ ՉԻ կարող ոչ ոքի "
                f"արգելափակել։ Խնդրում ենք ադմինի կարգավորումներում միացնել այս իրավունքը։",
            )
    except Exception:
        pass


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER))
async def on_bot_removed(event: ChatMemberUpdated):
    await set_channel_status(event.chat.id, active=False, can_ban=False)


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def on_bot_demoted(event: ChatMemberUpdated):
    # Բոտը մնում է ալիքում, բայց այլևս ադմին չէ (հանվել են իրավունքները) —
    # հաշվում ենք որպես ոչ ֆունկցիոնալ, մինչև նորից ադմին դառնա
    await set_channel_status(event.chat.id, active=False, can_ban=False)


# ---------------------------------------------------------------------------
# Օգտատերը փաստացի լքում է ալիքը → արգելափակում
# ---------------------------------------------------------------------------
# ԿԱՐԵՎՈՐ. այստեղ ՈՉ ՄԻ ChatMemberUpdatedFilter հարմար չէ, քանի որ դրանք
# հիմնվում են միայն նոր status-ի վրա։ Դա 2 սխալի պատճառ էր.
#   1) Կրկնակի հաշվարկ. երբ բոտն ինքն է բանում է օգտատիրոջը, դա ինքնին
#      ևս մեկ update է առաջացնում (left → kicked), որը կրկին համընկնում
#      էր հին ֆիլտրի հետ և կրկնակի հաշվում էր։
#   2) Ապաարգելափակման bug. երբ ադմինը ձեռքով հանում է oգտատիրոջը
#      Blocked users ցուցակից, դա status-ը փոխում է kicked → left, ինչը
#      հին ֆիլտրը սխալմամբ մեկնաբանում էր որպես «նոր լքում» և կրկին բանում։
# Լուծումը՝ ինքներս ստուգել, որ հին կարգավիճակը եղել է ԻՐԱԿԱՆ անդամություն
# (member/administrator/restricted-որպես-անդամ) և նոր կարգավիճակը left/kicked է։

REAL_MEMBER_STATUSES = {"member", "administrator", "creator"}
LEFT_STATUSES = {"left", "kicked"}


@router.chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    if old_status not in REAL_MEMBER_STATUSES or new_status not in LEFT_STATUSES:
        return  # ban-list կառավարում, restricted-փոփոխություն և այլն — չենք արձագանքում

    channel = await get_channel(event.chat.id)
    if not channel or not channel["enabled"] or not channel["can_ban"]:
        return

    user = event.old_chat_member.user
    if user.is_bot:
        return

    # Idempotency-ստուգում. եթե օգտատերն արդեն kicked է, չկրկնօրինակենք հաշվարկը
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
    text = (
        "👋 Բարի գալուստ։\n\n"
        "🛡 Այս բոտն ավտոմատ կերպով արգելափակում է այն օգտատերերին, ովքեր լքում են քո ալիքը "
        "(եթե բոտն ավելացված է որպես ադմին)։\n\n"
        "📋 «Իմ ալիքները» — կառավարիր քո ալիքները (միացնել/անջատել, տես քո ալիքի "
        "արգելափակումների քանակը)։\n"
    )
    if message.from_user.id == OWNER_ID:
        text += (
            "📊 «Ընդհանուր վիճակագրություն» — միայն դու ես տեսնում, "
            "թե ընդհանուր քանի ալիք է ներկայումս օգտագործում բոտը (որպես ադմին)։\n"
        )
    text += "\nԸնտրիր ցանկից՝"
    await message.answer(text, reply_markup=main_menu_kb(message.from_user.id))


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
            [InlineKeyboardButton(text="📊 Այս ալիքի վիճակագրություն", callback_data=f"stats_{chat_id}")],
            [InlineKeyboardButton(text="⬅️ Հետ", callback_data="my_channels")],
        ]
    )
    await callback.message.edit_text(
        f"Ալիք՝ {channel['chat_title']}\n"
        f"Կարգավիճակ՝ {status}\n\n"
        f"ℹ️ «Փոխարկել» միացնում/անջատում է ավտոմատ արգելափակումը միայն այս ալիքի համար։\n"
        f"ℹ️ «Վիճակագրություն»-ը ցույց է տալիս, թե քանի մարդ արդեն արգելափակվել է հենց այս ալիքից։",
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
        f"Այս ալիքում արգելափակված է {channel['banned_count']} օգտատեր։", show_alert=True
    )


@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    # Այս վիճակագրությունը երևում է ՄԻԱՅՆ OWNER_ID-ին՝ քանի ալիք ունի բոտն ընդհանուր առմամբ
    # ֆունկցիոնալ ադմին կարգավիճակով (ի տարբերություն "Վիճակագրություն"-ի, որը ցույց է
    # տալիս յուրաքանչյուր օգտատիրոջ ՄԻԱՅՆ իր սեփական ալիքի արգելափակումների քանակը)։
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Դու իրավունք չունես տեսնելու սա։", show_alert=True)
        return
    count = await count_active_channels()
    await callback.answer(
        f"🤖 Բոտը ներկայումս ադմին է {count} ալիքում։", show_alert=True
    )


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

    try:
        # Ամեն մեկնարկի ժամանակ (նաև redeploy-ից/update-ից հետո) իրական
        # ստուգում ենք Telegram API-ից՝ ուղղելու ցանկացած բացթողած update
        initial_count = await verify_all_channels(bot)
        logging.info(f"Մեկնարկային ստուգում ավարտվեց. ֆունկցիոնալ ալիքներ՝ {initial_count}")

        # Ֆոնային պարբերական ինքնաստուգում՝ շարունակական ճշգրտության համար
        asyncio.create_task(periodic_channel_check(bot))

        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(
            bot, allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
        )
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
